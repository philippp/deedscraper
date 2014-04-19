#!/usr/bin/env python
import csv
from datetime import datetime 
import pprint
import sys
import deedScraperLib as ds
import logging
import traceback

output_file_name    = './deed_scraper.out'
error_file_name     = './deed_scraper.err'  

def usage():
    print
    print 'Usage: ./deedScraper YYYYMMDD:YYYYMMDD'
    print
    print 'Writes output file containing details of the deeds ', output_file_name
    print 'Writes error file containing block/lot numbers where deeds could not be obtained', error_file_name
    print
    print
    sys.exit(2)

def parse_commandline_arguments(argv):
    dates = list()
    try:
        if len(argv) < 2:
            usage()

        if len(argv[1]) != 17 or argv[1][8] != ':':
            raise Exception("Incorrect date format:", arvg[1])

        # Converting date to MMDDYYYY.
        dates.append(argv[1][4:8] + argv[1][0:4])
        dates.append(argv[1][13:18] + argv[1][9:13])

    except Exception, e:
        print str(e)
        usage()

    return (dates[0], dates[1])



def main(argv):
    logging.basicConfig(level=logging.INFO)
    (date_start, date_end) = parse_commandline_arguments(argv)
    start = datetime.now()
    output_file = open(output_file_name, 'w')
    
    records = ds.fetch_records_for_daterange(date_start, date_end)
    pprint.pprint(records)
    end = datetime.now()
    timetaken = end - start

if __name__ == '__main__':
    main(sys.argv)
