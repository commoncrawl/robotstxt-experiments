import argparse
import logging

from pyathena import connect
from pyathena.util import RetryConfig


logging.basicConfig(level='INFO',
                    format='%(asctime)s %(levelname)s %(name)s: %(message)s')


temp_view_template = """
CREATE OR REPLACE VIEW {database}._tmp_view AS
WITH allrobots AS (
  SELECT topdomains.host as host,
         topdomains.domain as domain,
         topdomains.rank as rank,
         cc.url_host_tld,
         cc.url_host_registered_domain,
         cc.url_host_name,
         cc.url,
         cc.url_path,
         cc.url_query,
         cc.fetch_time,
         cc.fetch_status,
         cc.warc_filename,
         cc.warc_record_offset,
         cc.warc_record_length,
         cc.fetch_redirect,
         cc.content_mime_type,
         cc.content_mime_detected,
         -- enumerate records of same URL, most recent first
         ROW_NUMBER() OVER(PARTITION BY cc.url ORDER BY cc.fetch_time DESC) AS n
  FROM "ccindex"."ccindex" AS cc
    RIGHT OUTER JOIN "robotsexperiments"."topdomains" AS topdomains
    ON topdomains.host = cc.url_host_name
  WHERE cc.crawl = '{crawl}'
    AND cc.subset = 'robotstxt'
    AND cc.url_path = '/robots.txt'
    AND cc.url_query IS NULL)
SELECT *
 FROM allrobots
-- select only the first (most recent) record of the same URL
WHERE allrobots.n = 1;
"""

query_template = """
CREATE TABLE {database}._tmp_export
WITH (external_location = '{s3_location}/crawl={crawl}/',
      format = 'PARQUET',
      write_compression = 'ZSTD')
AS SELECT *
FROM {database}._tmp_view
"""

drop_tmp_table = 'DROP TABLE `{database}._tmp_export`;'


parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('s3_output_location',
                    help='output location on S3')
parser.add_argument('s3_staging_dir',
                    help='staging directory on S3 used for temporary and query metadata')
parser.add_argument('--database',
                    help='Name of the database', default='robotsexperiments')
parser.add_argument('crawl_data_set', nargs='+',
                    help='Common Crawl crawl dataset(s) to process, eg. CC-MAIN-2022-33')
args = parser.parse_args()


args.s3_output_location = args.s3_output_location.rstrip('/') # no trailing slash!

retry_config = RetryConfig(attempt=3)
cursor = connect(s3_staging_dir="{}".format(args.s3_staging_dir),
                 retry_config=retry_config,
                 region_name="us-east-1").cursor()


for crawl in args.crawl_data_set:

    query = temp_view_template.format(crawl=crawl,
                                      database=args.database,
                                      s3_location=args.s3_output_location)
    logging.info("Athena create view query: %s", query)
    cursor.execute(query)
    logging.info("Create view: %s", cursor.result_set.state)

    query = query_template.format(crawl=crawl,
                                  database=args.database,
                                  s3_location=args.s3_output_location)
    logging.info("Athena export query: %s", query)
    cursor.execute(query)

    logging.info("Athena query ID %s: %s",
                 cursor.query_id,
                 cursor.result_set.state)
    logging.info("       data_scanned_in_bytes: %d",
                 cursor.result_set.data_scanned_in_bytes)
    logging.info("       total_execution_time_in_millis: %d",
                 cursor.result_set.total_execution_time_in_millis)

    cursor.execute(drop_tmp_table.format(database=args.database))
    logging.info("Drop temporary table: %s", cursor.result_set.state)

