Deedscraper
===========

Pulls details of property deeds for the sale of properties in the San Francisco city+county area from www.criis.com

Invocation ./deedScraper.py YYYYMMDD:YYYYMMDD output_path

The first argument specifies the start and end days of our query, which are inclusive and may be the same date for a single day query. We store records (see "record format" section below) encoded as JSON lists in the directory specified by output_path. Storing each day in its own file should make it easier to resume fetching upon failure.


Records
-------

Records contain the following fields:

    * id (string): Record Document ID
    * date (string): Date of filing
    * reel_image (string): Reel and image of original document
    * doctype (string): Document type (Currently just DEED)
    * apn (list of strings): Block and Lot numbers of the record
    * grantors (list of strings): Grantors of the deed
    * grantees (list of strings): Grantees (recipients) of the deed

Warning
-------

Do not browse www.criis.com while running deedScrapper as this may interfere with deedScraper. This is due to the way the www.criis.com website ahs been coded.


Ethics for running
-------------------

Testing has shown that it takes about 0.7 sec to pull all the deed details for a block/lot. The total number of block/lot numbers is approximately 200K. Therefore, it will need to be run for approximately 39 hours.

It is absolutely essential that running deedScraper does not impact the operation of www.criss.com. The following precautions have/should be taken:

    * Browser details on each request contain an email address allowing www.criss.com to contact us in the event of problems.
    * The number of blocks/lot numbers has slowly been increased from 1 to 200 to 10000. The website www.criss.com has continued to remain responsive
    * Long runs (e.g. > 15 mins) should take place 12pm - 8am when load on the website will be low minimizing the impact
    * deedScraper has a throttle to slow down requests. Defaults 200ms delay per block/lot number. At the moment it seems that www.criis.com has   appropriate throttling in place and it is redundant.
    * Terms of use have been download from www.criss.com. No restrictions on automated downloads of data.




