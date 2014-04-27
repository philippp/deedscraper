import httplib, urllib
import re
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
def fetch_records_for_daterange(start_date, end_date, record_type_num):
    date_query_caller = CRIISCallerDateQuery()
    date_query_parser = HTMLRecordsDateQueryParser()

    apn_query_caller = CRIISCallerAPNQuery()
    apn_query_parser = HTMLRecordsAPNParser()

    date_query_retries = 0
    date_query_max_retries = 3
    html_daterecords = None
    while date_query_retries < date_query_max_retries:
        try:
            html_daterecords = date_query_caller.fetch(
                start_date, end_date, record_type_num)
            break
        except DSException:
            logging.error("Caught a DSException fetching dates %s to %s " % (
                    start_date, end_date))
            date_query_caller.close_connection()
            time.sleep(5)
            date_query_caller.create_connection()
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
        logging.info("Looking up APNs for %s (%s)", joinkey,
                     normalized_record['date'])
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
                apn_query_caller.create_connection()

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

""" The system we're calling was built in the 90s, so it sometimes has
issues. This exception indicates a recoverable failure from criis.com. """
class DSException(Exception):
    def __init__(self, value):
        logging.error(value)
        Exception.__init__(self, value)

RECORD_TYPES = {
    "DEED" : "001",
    "DEED_OF_TRUST" : "002",
    "DEED_OF_TRUST_WITH_RENTS" : "003",
}

""" Welcome to 1990. CRiis gets a POST request, calls a back-end
called CyberQuery that writes the result into a world-browsable directory
and then redirects you to the results file.

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
                logging.error("Retrying in %f seconds", sleep_sec)
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

    def fetch(self, date_start, date_end, doc_type="001"):
        params = urllib.urlencode({
                'DOC_TYPE': doc_type,
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
        self.column = -1
        self.in_font = False
        self.data = dict()
        self.records = dict()
        self.join_key = "Document"  # The column we join APNs and records on.
        self.is_apn = False
        self.column_to_field = dict()

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
            self.column = -1
        elif tag == 'tr':
            self.column = -1
        elif tag =='td':
            self.column += 1
        elif tag == 'font' and HTMLRecordsParser.get_attribute(attrs, 'color') == None:
            self.in_font = True

    def handle_endtag(self, tag):
        if tag == 'table' and self.in_records_table:
            self.in_records_table = False
        elif tag == 'tr' and self.in_records_table:
            self.flush_data_to_records()
        elif tag == 'font':
            self.in_font = False
            
    """Called with data in HTML tags. Populates the records dict with
    a list of extracted rows for every joinkey (Document ID) encountered.
    It is assumed that every field can have multiple entries (ex: grantees,
    APNs). """
    def handle_data(self, celldata):
        if self.in_records_table and self.in_font:
            if self.column in self.column_to_field:
                fieldname = self.column_to_field[self.column]
                if fieldname in self.data.keys():
                    self.data[fieldname].append(celldata)
                else:
                    self.data[fieldname] = [celldata]

    def flush_data_to_records(self):
        required_keys = self.column_to_field.values()
        for k in required_keys:
            if k not in self.data.keys():
                self.data = dict()
                return False;

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
        return True

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

    """Validate a DateQuery-generated records against common parsing issues."""
    @staticmethod
    def validate_records(records):
        data_valid = True
        for key, val in records.iteritems():
            for v in val:                
                if not v['Document'] == key:
                    logging.error("Document ID %s did not match joinkey %s",
                                  v['Document'], key)
                    data_valid = False

                # Check the Date
                dateval = v.get('RecordDate')
                if not dateval:
                    data_valid = False
                    logging.error("Record for %s had no date: %s",
                                  key, str(v))
                elif not re.match("\d{2}\/\d{2}\/\d{4}", v['RecordDate']):
                        data_valid = False
                        logging.error("Corrupted date in record %s: %s",
                                  key, dateval)
                # Check the DocType
                if v['DocType'].replace(" ", "_") not in RECORD_TYPES.keys():
                    data_valid = False
                    logging.error("Corrupted DocType in record %s: %s",
                                  key, v['DocType'])
                
                if ' ' in v['APNLink']:
                    data_valid = False
                    logging.error("Invalid APNLink: %s", v['APNLink'])

                if v['GrantorGrantee'] not in ('E', 'R'):
                    data_valid = False
                    logging.error("Expected GrantorGrantee to be E or R.")

        return data_valid

""" Parser for HTML pages listing APNs of records. """
class HTMLRecordsAPNParser(HTMLRecordsParser):
    def __init__(self):
        HTMLRecordsParser.__init__(self)
        self.is_apn = True
        # APN Pages are malformed when the images and reels are not populated.
        # When that's the case, we need to sort out what's what manually.
        self.all_data_for_sniffing = []
        self.column_to_field = {
            1: 'Document',
            3: 'Reel',
            4: 'Image',
            9: 'APN' }

    """Validate a APN query generated records against common parsing issues."""
    def validate_records(self, records):
        data_valid = True
        if len(records.keys()) == 0:
            logging.error("No records found")
            data_valid = False
        for key, records in records.iteritems():
            for record in records:
                for colname in self.column_to_field.values():
                    if colname not in record.keys():
                        logging.error("Found record without %s entry: %s",
                                      colname, str(record))
                        data_valid = False
                if not re.match("[\d\w]+\-\d+", record['APN']):
                    logging.error("Invalid APN (block and lot) for %s: %s",
                                  key, record['APN'])
        return data_valid

    def handle_data(self, celldata):
        if self.in_records_table and celldata:
            self.all_data_for_sniffing.append(celldata)
        HTMLRecordsParser.handle_data(self, celldata)

    def flush_data_to_records(self):
        # Sometimes we don't get Reel/Image entries here, and the
        # cells are missing font tags. If that's the case, we need to sniff
        # out the data.
        required_keys = self.column_to_field.values()
        sniffing_needed = False
        for k in required_keys:
            if k not in self.data.keys():
                sniffing_needed = True

        if sniffing_needed:
            # Some new records don't have Image and Reel yet, and the respective
            # cells miss the font tags and the table has extra cells (WTF?!?)
            # which breaks our nice column mapping.
            newdata = {'Image':[], 'Reel':[]}

            # The year looks JUST like an image number, so we first find the
            # date, extract its year and toss the YYYY year out.
            year = ""
            for d in self.all_data_for_sniffing:
                result = re.search("\d{2}\/\d{2}\/(\d{4})", d)
                if result:
                    year = result.group(1)
            if year:
                self.all_data_for_sniffing.remove(year)

            # Order here is crucial: Most restrictive to most permissive.
            keys_regexps = (
                ("Document", "[A-Z]\d+\-\d{2}"),
                ("APN", "[\d\w]+\-\d+"),
                ("Reel", "[A-Z]\d{2,4}"),
                ("Image", "\d{4}"))

            for value in self.all_data_for_sniffing:
                for k, r in keys_regexps:
                    if re.match(r, value):
                        if k in newdata.keys():
                            newdata[k].append(value)
                        else:
                            newdata[k] = [value]
                        break
            self.data = newdata
        self.all_data_for_sniffing = list()
        HTMLRecordsParser.flush_data_to_records(self)
