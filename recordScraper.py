#!/usr/bin/env python
import csv
from datetime import datetime 
import pprint
import sys
import recordScraperLib as rs
import logging
import datetime
import json

def usage():
    print
    print 'Usage: ./recordScraper YYYYMMDD:YYYYMMDD RECORDTYPE data_path'
    print
    print """Fetches records between the two specified dates and writes output files containing details of the records into the directory specified by data_path. Output files are JSON-encoded deed data and are chunked per day."""
    sys.exit(2)

def parse_commandline_arguments(argv):
    try:
        if len(argv) < 4:
            usage()

        if len(argv[1]) != 17 or argv[1][8] != ':':
            raise Exception("Incorrect date format:", arvg[1])

        if not rs.RECORD_TYPES.get(argv[2]):
            print "Invalid record type, valid types are:"
            pprint.pprint(rs.RECORD_TYPES.keys())
            raise Exception()
    except Exception, e:
        print str(e)
        usage()
    return (argv[1][0:8], argv[1][9:18], argv[2], argv[3])

""" Given two YYYYMMDD formatted date strings, return a list containing
all days in this range (including end date) in MMDDYYYY format."""
def expand_dates_to_MMDDYYYY_list(start_date_str, end_date_str):
    mmddyyyy_list = list()
    start_date = datetime.date(int(start_date_str[0:4]),
                               int(start_date_str[4:6]),
                               int(start_date_str[6:8]))
    end_date = datetime.date(int(end_date_str[0:4]),
                             int(end_date_str[4:6]),
                             int(end_date_str[6:8]))

    cur_date = start_date
    while cur_date <= end_date:
        mmddyyyy_list.append("%02d%02d%04d" % (
                cur_date.month,
                cur_date.day,
                cur_date.year))
        cur_date += datetime.timedelta(days=1)
    return mmddyyyy_list

def convert_mmddyyyy_to_output_filename(output_path, prefix, mmddyyyy_str):
    year_str = mmddyyyy_str[4:8]
    day_str = mmddyyyy_str[2:4]
    month_str = mmddyyyy_str[0:2]
    return output_path + ("/%s_%s%s%s.json" % (
            prefix, year_str, month_str, day_str))

def main(argv):
    logging.basicConfig(level=logging.INFO)
    (date_start, date_end, record_type_name, output_path) = \
        parse_commandline_arguments(argv)
    record_type_num = rs.RECORD_TYPES[record_type_name]
    date_list = expand_dates_to_MMDDYYYY_list(date_start, date_end)
    logging.info("Attempting to fetch for dates: %s", ",".join(date_list))
    start = datetime.datetime.now()
    idx = 0
    while idx < len(date_list):
        cur_date = date_list[idx]
        logging.info("Fetching records for %s", cur_date)
        try:
            records = rs.fetch_records_for_daterange(
                cur_date, cur_date, record_type_num)
        except rs.DSException:
            # Criis.com repeatedly timing out or throwing errors results
            # in a _BAD_READ file for that date, indicating that fetching failed.
            # Empty files OTOH are due to no filings on that date (ex: Sunday,
            # holidays).
            tombstone = convert_mmddyyyy_to_output_filename(
                output_path, record_type_name, cur_date) + "_BAD_READ"
            f_out = open(tombstone, 'w')
            f_out.write("FAILED TO READ")
            f_out.close()
            logging.error("Failed to fetch %s" % cur_date)
            continue

        f_out = open(convert_mmddyyyy_to_output_filename(
                output_path, record_type_name, cur_date), 'w')
        f_out.write(json.dumps(records))
        f_out.close()
        idx += 1
    end = datetime.datetime.now()
    timetaken = end - start
    logging.info("Processed %d dates in %d seconds." % (
            len(date_list), timetaken.seconds))

if __name__ == '__main__':
    main(sys.argv)
