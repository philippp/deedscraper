#!/usr/bin/env python

import csv
import os
import unittest
import pprint
import logging
import recordScraper as rs
import recordScraperLib as rsl

class TestDeedScraperFunctions(unittest.TestCase):
    def test_expand_dates_to_MMDDYYYY_list_singledate(self):
        single_dates_range = ("20130102", "20130102")
        reversed_dates_list = rs.expand_dates_to_MMDDYYYY_list(
            *single_dates_range)
        self.assertEqual(len(reversed_dates_list), 1)
        self.assertEqual(reversed_dates_list[0], "01022013")

    def test_expand_dates_to_MMDDYYYY_list_daterange(self):
        single_dates_range = ("20121231", "20130102")
        reversed_dates_list = rs.expand_dates_to_MMDDYYYY_list(
            *single_dates_range)
        self.assertEqual(len(reversed_dates_list), 3)
        self.assertEqual(reversed_dates_list[0], "12312012")
        self.assertEqual(reversed_dates_list[1], "01012013")
        self.assertEqual(reversed_dates_list[2], "01022013")

    def test_convert_mmddyyyy_to_output_filename(self):
        filename = rs.convert_mmddyyyy_to_output_filename(
            "outpath", "prefix", "01022013")
        self.assertEqual(filename, "outpath/prefix_20130102.json")

class TestHTMLRecordsDateQueryParser(unittest.TestCase):
    def test_get_attribute(self):
        self.assertEqual(rsl.HTMLRecordsParser.get_attribute(
                [], 'r'), None)
        self.assertEqual(rsl.HTMLRecordsParser.get_attribute(
                [('a', 'b'), ('c', 'd'), ('e', 'f')], 'c'), 'd')

    def test_parse_html(self):
        for c in ['1', '2', '3']:
            f = open('./testdata/datequery_doc_type_list' + c + '.html', 'r')
            datequery_parser = rsl.HTMLRecordsDateQueryParser()
            datequery_parser.feed(f.read())
            records = datequery_parser.get_records()
            self.assertEqual(
                rsl.HTMLRecordsDateQueryParser.validate_records(records), True)

class TestHTMLRecordsAPNParser(unittest.TestCase):
    def test_parse_html(self):
        for c in ['1', '2', '3']:
            f = open('./testdata/apnquery_doc_detail' + c + '.html', 'r')
            apn_parser = rsl.HTMLRecordsAPNParser()
            apn_parser.feed(f.read())
            records = apn_parser.get_records()
            self.assertEqual(
                apn_parser.validate_records(records), True)
            
if __name__ == '__main__':
    unittest.main()


