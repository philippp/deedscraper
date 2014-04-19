import httplib, urllib
import csv
from HTMLParser import HTMLParser
import logging

""" The system we're calling was built in the 90s, so it sometimes has
issues. This exception indicates a recoverable failure from criis.com."""
class DSException(Exception):
       def __init__(self, value):
        Exception.__init__(self, value)

""" Welcome to 1990. CRiis gets a POST request, calls a back-end
called CyberQuery that writes the result into a world-browsable directory
and then redirects you to the results file. I wish I were joking.

Note that the http_connection cannot be shared concurrently between two
CRIISCallers.
"""
class CRIISCaller(object):
    def __init__(self, http_connection):
        self.conn = http_connection
        self.default_headers = {
            'Content-type': 'application/x-www-form-urlencoded', 
            'Accept':       'text/html',
            'User-Agent':   'sararcher@outlook.com'
        }

    """ Returns string contents of the page. """
    def call_criis_with_redirection(self, url, params, headers=None):
        if not headers:
            headers = self.default_headers

        logging.info('Requesting %s %s %s', url, params, headers)
        self.conn.request('POST', url, params, headers)
        response = self.conn.getresponse()
        logging.info("Received response to %s", url)

        if response.status != 302:
            raise DSException('No redirect returned.') 

        redirect_url = response.getheader('Location')
        logging.info('Requesting %s', redirect_url)
        self.conn.request('GET', redirect_url)
        response = self.conn.getresponse()
        logging.info('Received response to %s', redirect_url)

        if response.status != 200:
            raise DSException('Post-redirect page fetching failed.') 
        return response.read()

""" Issues a date-range query to CRIIS. """
class CRIISCallerDateQuery(CRIISCaller):
    def __init__(self, http_connection):
        CRIISCaller.__init__(self, http_connection)

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


def parse_datequery_record_list(data):
    data_trimmed = ""
    # Broken tags and no data in the header, so we skip it.
    for line in data.split("\n"):
        if len(data_trimmed) == 0:
            if "<body " not in line:
                continue
            data_trimmed += "<html>\n"
        data_trimmed += line + "\n"
    parser = HTMLRecordsDateQueryParser()
    parser.feed(data_trimmed)
    records = parser.get_records()
    return records

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
    It's assumed that every field can have multiple entries (ex: grantees,
    APNs). """
    def handle_data(self, celldata):
        if self.in_records_table and self.in_font:
            print self.column, celldata
            # We process the last record when we return to column 0
            if self.column == 0:
               if self.join_key in self.data.keys():
                   for k in self.data.keys():
                       # Sometimes there are multiple cells in a row/col
                       # not sure of a better way to treat this
                       self.data[k] = "\n".join(self.data[k])
                   joinkeyvalue = self.data[self.join_key]
                   if joinkeyvalue in self.records.keys():
                       self.records[joinkeyvalue].append(self.data)
                   else:
                       self.records[joinkeyvalue] = [self.data]
               self.data = dict()
            if self.column in self.column_to_field:
                fieldname = self.column_to_field[self.column]
                if fieldname in self.data.keys():
                    self.data[fieldname].append(celldata)
                else:
                    self.data[fieldname] = [celldata]

    """ Provides records after page is parsed. """
    def get_records(self):
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

    """In addition to determining when we're in the table, we need to extract
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
        self.column_to_field = {
            2: 'Document',
            3: 'Reel',
            5: 'Image',
            6: 'APN' }

def write_data(file_obj, block, lot, data, parties):
    writer = csv.writer(file_obj, quoting=csv.QUOTE_ALL)

    for party in parties:
        writer.writerow([block, lot, data['Year'], data['Document'], data['RecordDate'],
                data['Reel'], data['Image'], data['DocumentType'], party[0], party[1]])

    file_obj.flush




