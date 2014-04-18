import httplib, urllib
import csv
from HTMLParser import HTMLParser
import logging

class DSException(Exception):
       def __init__(self, value):
        Exception.__init__(self, value)

def request_record_list(conn, date_start, date_end):
    headers = {
        'Content-type': 'application/x-www-form-urlencoded', 
        'Accept':       'text/html',
        'User-Agent':   'sararcher@outlook.com'
    }
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
    
    logging.info('Requesting /cgi-bin/new_get_recorded.cgi %s %s', str(params), str(headers))
    conn.request('POST', '/cgi-bin/new_get_recorded.cgi', params, headers)
    response = conn.getresponse()
    logging.info('Received response to /cgi-bin/new_get_recorded.cgi')

    if response.status != 302:
        raise DSException('request_deed_list - No redirect returned') 

    redirect_url = response.getheader('Location')

    logging.info('Requesting %s', redirect_url)
    conn.request('GET', redirect_url)
    response = conn.getresponse()
    logging.info('Received response to %s', redirect_url)

    if response.status != 200:
        raise DSException('request_deed_list - Get request failed') 

    return response.read()
  

def get_attribute(list, attribute):
    for item in list:
        if item[0] == attribute:
            return item[1]
    return None

def parse_datequery_record_list(data):
    data_trimmed = ""
    # Broken tags and no data in the header, so we skip it.
    for line in data.split("\n"):
        if len(data_trimmed) == 0:
            if "<body " not in line:
                continue
            data_trimmed += "<html>\n"
        data_trimmed += line + "\n"
    parser = RecordDateQueryParser()
    parser.feed(data_trimmed)
    records = parser.get_records()
    return records

class RecordDateQueryParser(HTMLParser):
    def __init__(self):
        HTMLParser.__init__(self)
        self.in_records_table = False
        self.record = -1
        self.column = -1
        self.data_row = False
        self.in_font = False
        self.data = dict()
        self.column_to_field = {
            2: 'RecordDate',
            3: 'Document',
            4: 'DocType',
            5: 'GrantorGrantee',
            6: 'Name' }
        self.records = dict()

    def handle_starttag(self, tag, attrs):
        if tag == 'table' and get_attribute(attrs, 'class') == 'records': 
            self.in_records_table = True
            self.record = -1
            self.column = -1
        elif tag == 'tr':
            self.record +=1
            self.column = -1
        elif tag =='td':
            self.column += 1
        elif tag == 'font' and get_attribute(attrs, 'color') == None:
            self.in_font = True
           
    def handle_endtag(self, tag):
        if tag == 'table' and self.in_records_table:
            self.in_records_table = False
        elif tag == 'tr':
            self.data_row = False
        elif tag == 'font':
            self.in_font = False

    def handle_data(self, celldata):
        if self.in_records_table and self.in_font:
           print self.column, celldata
           # We process the last record when we return to column 0
           if self.column == 0:
               if 'Document' in self.data.keys():
                   for k in self.data.keys():
                       # Sometimes there are multiple cells in a row/col
                       # not sure of a better way to treat this
                       self.data[k] = "\n".join(self.data[k])
                   docid = self.data['Document']
                   if docid in self.records.keys():
                       self.records[docid].append(self.data)
                   else:
                       self.records[docid] = [self.data]
               self.data = dict()
           if self.column in self.column_to_field:
               fieldname = self.column_to_field[self.column]
               if fieldname in self.data.keys():
                   self.data[fieldname].append(celldata)
               else:
                   self.data[fieldname] = [celldata]

    def get_records(self):
        return self.records

def write_data(file_obj, block, lot, data, parties):
    writer = csv.writer(file_obj, quoting=csv.QUOTE_ALL)

    for party in parties:
        writer.writerow([block, lot, data['Year'], data['Document'], data['RecordDate'],
                data['Reel'], data['Image'], data['DocumentType'], party[0], party[1]])

    file_obj.flush




