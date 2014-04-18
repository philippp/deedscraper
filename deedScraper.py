#!/usr/bin/env python
import csv
from datetime import datetime 
import httplib, urllib
import pprint
import sys
import time
import deedScraperLib as ds
import logging
import socket
import traceback

output_file_name    = './deed_scraper.out'
error_file_name     = './deed_scraper.err'  
throttle_default    = 200

def usage():
    print
    print 'Usage: ./deedScraper YYYYMMDD:YYYYMMDD THROTTLE'
    print
    print 'INPUT_FILENAME is a CSV file with 2 columns - the first is block number and the second is lot number'
    print 'THROTTLE is time delay per request - defaults to ', throttle_default, 'ms'
    print 
    print 'Writes output file containing details of the deeds ', output_file_name
    print 'Writes error file containing block/lot numbers where deeds could not be obtained', error_file_name
    print
    print
    sys.exit(2)

def parse_commandline_arguments(argv):
    dates = list()
    throttle = throttle_default

    try:
        if len(argv) < 2:
            usage()

        if len(argv[1]) != 17 or argv[1][8] != ':':
            raise Exception("Incorrect date format:", arvg[1])

        # Converting date to MMDDYYYY.
        dates.append(argv[1][4:8] + argv[1][0:4])
        dates.append(argv[1][13:18] + argv[1][9:13])

        if len(argv) == 3:
            throttle = float(argv[2])

    except Exception, e:
        print str(e)
        usage()

    return (dates[0], dates[1], throttle)

def create_connection():
    website = 'www.criis.com'
    conn    =  httplib.HTTPConnection(website, timeout=10)    
    print 'Connection to ', website, ' opened'
    return conn

def main(argv):
    logging.basicConfig(level=logging.INFO)
    (date_start, date_end, throttle) = parse_commandline_arguments(argv)
    print 'Throttle between requests ', throttle, 'ms'
    
    conn = create_connection()
    time_out_errors = 0
    start = datetime.now()
    output_file = open(output_file_name, 'w')
    obtained_records = False
    for retry in range(0, 3):
        # Throttle to ensure we do not overload website
        time.sleep(throttle / 1000.0)
        try:
            document = ds.request_record_list(conn, date_start, date_end)
            records = ds.parse_datequery_record_list(document)
            pprint.pprint(records)
            obtained_records = True
            break

        except socket.timeout, e:
            conn.close()
            conn = create_connection()
            logging.error('timed out: %s', str(e))
            logging.info(traceback.format_exc())
            logging.error('retrying...')

        if not obtained_records:
            time_out_errors += 1
            if time_out_errors < 10:
                raise ds.DSException('Timeout after 3 attempts')
            else:
                raise Exception('There have been 10 timeout errors - aborting')

    end = datetime.now()
    timetaken = end - start

if __name__ == '__main__':
    main(sys.argv)


