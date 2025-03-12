"""Find robots.txt captures for a list of host names in Common Crawl's
robots.txt dataset."""

import argparse
import logging
import os

from collections import Counter, defaultdict
from urllib.parse import urljoin, urlparse

import pandas as pd

from pyathena import connect
from pyathena.util import RetryConfig


logging.basicConfig(level='INFO',
                    format='%(asctime)s %(levelname)s %(name)s: %(message)s')


temp_view_template = """
CREATE OR REPLACE VIEW {database}._tmp_view AS
WITH allrobots AS (
  -- note: the table topdomains is expected to be created ahead
  SELECT topdomains.host as host,
         topdomains.domain as domain,
         topdomains.rank as rank,
         cc.url as orig_url,
         cc.url_host_tld,
         cc.url_host_registered_domain,
         cc.url_host_name,
         cc.url,
         cc.url_protocol,
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

# template to save result on S3 as Parquet
query_template_save_parquet = """
CREATE TABLE {database}._tmp_export
WITH (external_location = '{s3_location}/crawl={crawl}/redirects={redir_depth}/',
      format = 'PARQUET',
      write_compression = 'ZSTD')
AS SELECT *
FROM {database}._tmp_view
"""

drop_tmp_table = 'DROP TABLE `{database}._tmp_export`;'


# templates to follow redirects
temp_view_template_redirects = """
CREATE OR REPLACE VIEW {database}._tmp_view AS
WITH allrobots AS (
  -- note: the table `redirects_to_follow` is expected to be created ahead
  --       on the output location of the redirect targets
  SELECT redir.host as host,
         redir.domain as domain,
         redir.rank as rank,
         redir.orig_url,
         cc.url_host_tld,
         cc.url_host_registered_domain,
         cc.url_host_name,
         cc.url,
         cc.url_protocol,
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
         redir.from_url,
         redir.from_fetch_status,
         redir.from_to_is_same_host,
         -- enumerate records of same <orig. host, URL>, most recent first
         ROW_NUMBER() OVER(PARTITION BY redir.host, cc.url ORDER BY cc.fetch_time DESC) AS n
  FROM "ccindex"."ccindex" AS cc
    RIGHT OUTER JOIN "robotsexperiments"."redirects_to_follow" AS redir
    ON redir.to_url = cc.url
  WHERE cc.crawl = '{crawl}'
    AND cc.subset = 'robotstxt'
    AND redir.redirects = {redir_depth})
SELECT *
 FROM allrobots
-- select only the first (most recent) record of the same URL
WHERE allrobots.n = 1;
"""


def initial_lookup(crawl, cursor, args):
    query = temp_view_template.format(crawl=crawl,
                                      database=args.database)
    logging.info("Athena create view query: %s", query)
    cursor.execute(query)
    logging.info("Create view: %s", cursor.result_set.state)

    query = query_template_save_parquet.format(crawl=crawl,
                                               database=args.database,
                                               s3_location=args.s3_output_location,
                                               redir_depth=0)
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


def redirect_targets_write(crawl, redir_depth, args, urls_seen):
    result_path_tmpl = '{s3_output_location}/crawl={crawl}/redirects={redir_depth}/'
    redirect_target_path_tmpl = '{s3_redirect_target_location}/crawl={crawl}/redirects={redir_depth}/redirects_to_follow-{redir_depth}-{crawl}.zstd.parquet'

    result_path = result_path_tmpl.format(s3_output_location=args.s3_output_location,
                                          crawl=crawl, redir_depth=redir_depth)
    target_path = redirect_target_path_tmpl.format(s3_redirect_target_location=args.s3_redirect_target_location,
                                                   crawl=crawl, redir_depth=redir_depth)

    counts = Counter()

    df = pd.read_parquet(result_path)

    counts['rows'] = df.shape[0]

    redirects = defaultdict(list)
    urls_seen.update(df['url'].tolist())
    logging.info('%6d\tunique URLs known', len(urls_seen))

    for _idx, row in df[~df['fetch_redirect'].isnull()].iterrows():

        redirect_target = row['fetch_redirect']
        counts['redirects'] += 1

        if redirect_target == '':
            counts['redirects_empty'] += 1
            continue # empty URL

        if (redirect_target.startswith('http://')
            or redirect_target.startswith('https://')):
            counts['redirects_absolute'] += 1
        else:
            counts['redirects_relative'] += 1
            redirect_target = urljoin(row['url'], redirect_target)

        if redirect_target == row['url']:
            # nothing to do for redirect targets pointing to the URL itself
            counts['redirects_self'] += 1
            continue

        if redirect_target in urls_seen:
            counts['redirects_target_known'] += 1
        elif redirect_target in redirects:
            counts['redirects_duplicates'] += 1
        else:
            counts['redirects_to_follow'] += 1
        # Append also known redirect targets because we need to track
        # the chain from the initial /robots.txt to the final location.
        # WARC records are deduplicated before download.
        redirects[redirect_target].append(row)


    logging.info('Redirects processed:')
    for cnt in counts:
        logging.info('%6d\t%s', counts[cnt], cnt)

    if len(redirects) == 0:
        return 0

    redirects_to_follow = list()
    for redirect_target in redirects:
        for row in redirects[redirect_target]:
            rrow = row[['host', 'domain', 'rank', 'orig_url',
                        'url', 'fetch_status']].tolist()
            target_host = None
            try:
                urlparse(redirect_target).hostname
            except ValueError as e:
                logging.error('Failed to parse redirect target `%s`: %s',
                              redirect_target, e)
            rrow.append(row['url_host_name'] == target_host)
            rrow.append(redirect_target)
            redirects_to_follow.append(rrow)

    df_redirects_to_follow = pd.DataFrame(redirects_to_follow,
                                          columns=['host', 'domain', 'rank', 'orig_url',
                                                   'from_url', 'from_fetch_status',
                                                   'from_to_is_same_host', 'to_url'])
    df_redirects_to_follow.to_parquet(target_path, compression='zstd')

    return len(redirects)

def redirect_targets_load_partitions(args):
    query = 'MSCK REPAIR TABLE `{database}`.`redirects_to_follow`;'.format(
        database=args.database)
    logging.info("Athena load partitions query: %s", query)
    cursor.execute(query)
    logging.info("Load partitions: %s", cursor.result_set.state)

def redirect_lookup(crawl, redir_depth, redir_depth_next, cursor, args):
    query = temp_view_template_redirects.format(crawl=crawl,
                                                database=args.database,
                                                redir_depth=redir_depth)
    logging.info("Athena create view query: %s", query)
    cursor.execute(query)
    logging.info("Create view: %s", cursor.result_set.state)

    query = query_template_save_parquet.format(crawl=crawl,
                                               database=args.database,
                                               s3_location=args.s3_output_location,
                                               redir_depth=redir_depth_next)
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


################################################################################
parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('s3_output_location',
                    help='Output location on S3')
parser.add_argument('s3_redirect_target_location',
                    help='Prefix on S3 used to hold redirect target locations required'
                    ' for table joins when following redirects')
parser.add_argument('s3_staging_dir',
                    help='Staging directory on S3 used for temporary and query metadata')
parser.add_argument('--database',
                    help='Name of the database', default='robotsexperiments')
parser.add_argument('--follow_redirects', type=int,
                    help='Follow up to n redirects', default=5)
parser.add_argument('crawl_data_set', nargs='+',
                    help='Common Crawl crawl dataset(s) to process, eg. CC-MAIN-2022-33')
args = parser.parse_args()

# no trailing slash on S3 prefixes!
args.s3_output_location = args.s3_output_location.rstrip('/')
args.s3_redirect_target_location = args.s3_redirect_target_location.rstrip('/')


retry_config = RetryConfig(attempt=3)
cursor = connect(s3_staging_dir="{}".format(args.s3_staging_dir),
                 retry_config=retry_config,
                 region_name="us-east-1").cursor()


for crawl in args.crawl_data_set:

    initial_lookup(crawl, cursor, args)

    # following redirects
    urls_seen = set() # for deduplication
    # RFC 9390 says "to follow at least five consecutive redirects"
    for i in range(0, 5):
        logging.info('Following redirects (from depth = %i)', i)
        target_count = redirect_targets_write(crawl, i, args, urls_seen)
        if target_count == 0:
            logging.info("No redirects found at level %i, stopping.", i)
            break
        redirect_targets_load_partitions(args)
        redirect_lookup(crawl, i, (i+1), cursor, args)
