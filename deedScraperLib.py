import httplib, urllib
import csv
from HTMLParser import HTMLParser
import logging
import pprint
import time
import socket
import traceback

SLEEP_THROTTLE = 200  # ms to sleep between requests
MULTILINE_WORKAROUND_KEY = "|||"

""" Fetch records for date range, including owner and APN information.
Returns a date-sorted list of normalized records.
"""
def fetch_records_for_daterange(start_date, end_date):
    date_query_caller = CRIISCallerDateQuery()
    date_query_parser = HTMLRecordsDateQueryParser()

    apn_query_caller = CRIISCallerAPNQuery()
    apn_query_parser = HTMLRecordsAPNParser()

    date_query_retries = 0
    date_query_max_retries = 3
    html_daterecords = None
    while date_query_retries < date_query_max_retries:
        try:
            html_daterecords = date_query_caller.fetch(start_date, end_date)
            break
        except DSException:
            logging.error("Caught a DSException fetching dates %s to %s " % (
                    start_date, end_date))
            date_query_caller.close_connection()
            time.sleep(5)
            date_query_caller.open_connection()
    if html_daterecords == None:
        raise DSException("Failed to fetch for date range %s to %s" % (
                    start_date, end_date))
    date_query_parser.feed(html_daterecords)
    denorm_records = date_query_parser.get_records()

    normalized_records = []

    run_cnt = 0
    # The date query returnes several rows for each record, and each
    # row can have several entries for each key.
    for joinkey, record_rows in denorm_records.iteritems():
        run_cnt += 1
        if len(record_rows) == 0:
            logging.warning("No record rows for joinkey %s", joinkey)
            continue
        record_rows_zero_keys = record_rows[0].keys()
        if "RecordDate" not in record_rows_zero_keys or \
                "DocType" not in record_rows_zero_keys or \
                "APNLink" not in record_rows_zero_keys:
            logging.warning("Encountered a bad row with keys: %s" % (
                    ",".join(record_rows_zero_keys)))
            continue
        normalized_record = dict()
        normalized_record['id'] = joinkey
        normalized_record['date'] = record_rows[0]['RecordDate']
        normalized_record['doctype'] = record_rows[0]['DocType']
        normalized_record['grantors'] = list()
        normalized_record['grantees'] = list()
        # Records can conceivably span two images, which may span two reels...
        normalized_record['reel_image'] = list()
        # Some deeds cover multiple APNs
        normalized_record['apn'] = list()

        # Fetch the grantors and grantees
        for row in record_rows:
            if not row.get('Name'):
                continue
            names = row['Name'].split(MULTILINE_WORKAROUND_KEY)
            if row.get('GrantorGrantee','') == 'E':
                normalized_record['grantees'] += names
            elif row.get('GrantorGrantee','') == 'R':
                normalized_record['grantors'] += names
        normalized_record['grantors'] = list(set(normalized_record['grantors']))
        normalized_record['grantees'] = list(set(normalized_record['grantees']))

        # Fetch the APNs.
        apn_url = record_rows[0]['APNLink']
        logging.info("Looking up APNs for %s via URL %s", joinkey, apn_url)
        apn_fetch_retries = 0
        apn_fetch_max_retries = 3
        while apn_fetch_retries < apn_fetch_max_retries:
            try:
                apn_list_html = apn_query_caller.fetch(apn_url)
                break
            except DSException:
                logging.error("Caught a DSException trying to fetch %s." % (
                        apn_url))
                apn_query_caller.close_connection()
                time.sleep(5)
                apn_query_caller.open_connection()

        apn_query_parser.feed(apn_list_html)
        denorm_apn_records = apn_query_parser.get_records()
        reel_image = list()
        apns = list()
        for apn_joinkey, apn_record_rows in denorm_apn_records.iteritems():
            if apn_joinkey != joinkey:
                logging.warning("APN fetching resulted in conflicting "
                                "document IDs: %s vs %s", joinkey, apn_joinkey)

            for apn_row in apn_record_rows:
                reel = apn_row.get('Reel','')
                image = apn_row.get('Image','')
                if reel and image:
                    normalized_record['reel_image'].append(reel + ',' + image)
                normalized_record['apn'] += apn_row.get('APN','').split(
                    MULTILINE_WORKAROUND_KEY)

        normalized_record['reel_image'] = list(set(normalized_record['reel_image']))
        normalized_record['apn'] = list(set(normalized_record['apn']))
        normalized_records.append(normalized_record)
    return normalized_records

def parse_datequery_record_list(data):
    parser.feed(data)
    records = parser.get_records()
    return records

""" The system we're calling was built in the 90s, so it sometimes has
issues. This exception indicates a recoverable failure from criis.com. """
class DSException(Exception):
    def __init__(self, value):
        logging.error(value)
        Exception.__init__(self, value)

""" Welcome to 1990. CRiis gets a POST request, calls a back-end
called CyberQuery that writes the result into a world-browsable directory
and then redirects you to the results file. I wish I were joking.

Note that the http_connection cannot be shared concurrently between two
CRIISCallers.
"""
class CRIISCaller(object):

    website = 'www.criis.com'

    def __init__(self):
        self.conn = None
        self.create_connection()
        self.default_headers = {
            'Content-type': 'application/x-www-form-urlencoded', 
            'Accept':       'text/html',
            'User-Agent':   'sararcher@outlook.com'
        }

    def __del__(self):
        self.close_connection()

    def create_connection(self):
        self.close_connection()
        self.conn = httplib.HTTPConnection(self.website, timeout=10)    
        logging.info('Connection to %s opened.', self.website)

    """ Call this when you're done with the object so we don't keep
    unused open HTTP connections."""
    def close_connection(self):
        if (self.conn != None):
            self.conn.close()
        self.conn = None

    """ Returns string contents of the page. """
    def call_criis_with_redirection(self, url, params, headers=None):
        if not headers:
            headers = self.default_headers
        logging.info('Requesting %s %s %s', url, params, headers)
        self.call_http_with_retries('POST', url, params, headers)
        response = self.get_response_with_retries()
        if response.status != 302:
            raise DSException('No redirect returned.') 

        redirect_url = response.getheader('Location')
        self.call_http_with_retries('GET', redirect_url)
        response = self.get_response_with_retries()
        if response.status != 200:
            raise DSException('Post-redirect page fetching failed.') 
        return response.read()

    # TODO: Refactor to avoid duplicating sleep logic and constants.
    def get_response_with_retries(self):
        sleep_per_retry_ms = 2000
        max_retries = 5
        for retry in range(0, max_retries):
            try:
                return self.conn.getresponse()
            except socket.timeout, e:
                logging.error('Timeout #%d: %s', retry+1, str(e))
                logging.info(traceback.format_exc())
                sleep_sec = sleep_per_retry_ms / 1000.0
                logging.error("Retrying in %2.2f seconds" % sleep_sec)
                time.sleep(sleep_sec)

        raise DSException(
            "Failed to getresponse after %d attempts. Bailing." % max_retries)
                

    def call_http_with_retries(self, req_type, url, params=None, headers=None):
        if not params:
            params = ""
        if not headers:
            headers = self.default_headers
        additional_sleep_per_retry_ms = 3000
        max_retries = 10
        # Pour one out for the underprovisioned homies.
        time.sleep(SLEEP_THROTTLE/1000.0)
        for retry in range(0, max_retries):
            try:
                self.conn.request(req_type, url, params, headers)
                return
            except socket.timeout, e:
                logging.error('Timeout #%d: %s', retry+1, str(e))
                logging.info(traceback.format_exc())
                sleep_sec = (retry + 1) * additional_sleep_per_retry_ms / 1000.0
                logging.error("Retrying in %2.2f seconds")
                time.sleep(sleep_sec)
                self.create_connection()
        raise DSException(
            "Failed to call %s after %d attempts. Bailing." % (url,max_retries))

""" Issues a date-range query to CRIIS. """
class CRIISCallerDateQuery(CRIISCaller):
    def __init__(self):
        CRIISCaller.__init__(self)

    def fetch(self, date_start, date_end):
        params = urllib.urlencode({
                'DOC_TYPE': '001',
                'doc_dateA': date_start,
                'doc_dateB': date_end,
                'SEARCH_TYPE': 'DOCTYPE',
                'COUNTY':       'sanfrancisco',
                'YEARSEGMENT':  'current',
                'ORDER_TYPE':   'Recorded Official',
                'LAST_RECORD': '1',
                'SCREENRETURN': 'doc_search.cgi',
                'SCREEN_RETURN_NAME': 'Recorded Document Search',
                })

        return self.call_criis_with_redirection(
            "/cgi-bin/new_get_recorded.cgi", params)

class CRIISCallerAPNQuery(CRIISCaller):
    def __init__(self):
        CRIISCaller.__init__(self)

    def fetch(self, apn_url):
        if "?" not in apn_url:
            raise DSException("Malformed APN Url: %s", apn_url)
        url, params = apn_url.split("?")
        return self.call_criis_with_redirection(url, params)

#
# HTML Parsers
#

""" Base class of a parser of city records served via criis.com HTML pages. """
class HTMLRecordsParser(HTMLParser):
    def __init__(self):
        HTMLParser.__init__(self)
        self.in_records_table = False
        self.record = -1
        self.column = -1
        self.in_font = False
        self.data = dict()
        self.records = dict()
        self.join_key = "Document"  # The column we join APNs and records on.
        self.is_apn = False

    """ Process criis.com page content. Call get_records() after this. """
    def feed(self, pagecontent):
        self.data = dict()
        self.records = dict()
        data_trimmed = ""
        # The criis.com header tags have broken html and no data, so we drop
        # everything before the body tag.
        for line in pagecontent.split("\n"):
            if len(data_trimmed) == 0:
                if "<body " not in line:
                    continue
                data_trimmed += "<html>\n"
            data_trimmed += line + "\n"
        HTMLParser.feed(self, data_trimmed)

    @staticmethod
    def get_attribute(list, attribute):
        for item in list:
            if item[0] == attribute:
                return item[1]
        return None

    def handle_starttag(self, tag, attrs):
        if tag == 'table' and HTMLRecordsParser.get_attribute(attrs, 'class') == 'records': 
            self.in_records_table = True
            self.record = -1
            self.column = -1
        elif tag == 'tr':
            self.record +=1
            self.column = -1
        elif tag =='td':
            self.column += 1
        elif tag == 'font' and HTMLRecordsParser.get_attribute(attrs, 'color') == None:
            self.in_font = True

    def handle_endtag(self, tag):
        if tag == 'table' and self.in_records_table:
            self.in_records_table = False
        elif tag == 'font':
            self.in_font = False
            
    """Called with data in HTML tags. Populates the records dict with
    a list of extracted rows for every joinkey (Document ID) encountered.
    It is assumed that every field can have multiple entries (ex: grantees,
    APNs). """
    def handle_data(self, celldata):
        if self.in_records_table and self.in_font:
            #print self.column, celldata
            # We process the last record when we return to column 0
            if self.column == 0:
                self.flush_data_to_records()
            if self.column in self.column_to_field:
                fieldname = self.column_to_field[self.column]
                if fieldname in self.data.keys():
                    self.data[fieldname].append(celldata)
                else:
                    self.data[fieldname] = [celldata]

    def flush_data_to_records(self):
        if not self.join_key in self.data.keys():
            return

        for k in self.data.keys():
            # Sometimes there are multiple cells in a row/col
            # not sure of a better way to treat this
            self.data[k] = MULTILINE_WORKAROUND_KEY.join(
                self.data[k])
            joinkeyvalue = self.data[self.join_key]
            if type(joinkeyvalue) == list:
                joinkeyvalue = joinkeyvalue[0]
            if joinkeyvalue in self.records.keys():
                self.records[joinkeyvalue].append(self.data)
            else:
                self.records[joinkeyvalue] = [self.data]
        self.data = dict()

    """ Provides records after page is parsed. """
    def get_records(self):
        # Flush any remaining data.
        self.flush_data_to_records()
        return self.records


""" Parser for HTML pages listing date-queried records. """
class HTMLRecordsDateQueryParser(HTMLRecordsParser):
    def __init__(self):
        HTMLRecordsParser.__init__(self)
        self.column_to_field = {
            2: 'RecordDate',
            3: 'Document',
            4: 'DocType',
            5: 'GrantorGrantee',
            6: 'Name' }

    """In addition to determining when we are in the table, we need to extract
    the APN link for date-queries records. """
    def handle_starttag(self, tag, attrs):
        HTMLRecordsParser.handle_starttag(self, tag, attrs)
        if tag=='a' and self.in_records_table and self.column == 1:
            href = HTMLRecordsParser.get_attribute(attrs, 'href')
            if href is not None:
                self.data['APNLink'] = [href]        

""" Parser for HTML pages listing APNs of records. """
class HTMLRecordsAPNParser(HTMLRecordsParser):
    def __init__(self):
        HTMLRecordsParser.__init__(self)
        self.is_apn = True
        self.column_to_field = {
            1: 'Document',
            3: 'Reel',
            4: 'Image',
            9: 'APN' }

def write_data(file_obj, block, lot, data, parties):
    writer = csv.writer(file_obj, quoting=csv.QUOTE_ALL)

    for party in parties:
        writer.writerow([block, lot, data['Year'], data['Document'], data['RecordDate'],
                data['Reel'], data['Image'], data['DocumentType'], party[0], party[1]])

    file_obj.flush




