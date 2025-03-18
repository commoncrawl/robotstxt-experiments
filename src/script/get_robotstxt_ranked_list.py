"""Extract list of hosts, their rank and robots.txt capture status
from the result of `get-robotstxt-captures-athena.py`.
"""

import argparse
import os
import logging

from urllib.parse import urljoin

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from get_robotstxt_download_list import is_robotstxt_mime_type


logging.basicConfig(level='INFO',
                    format='%(asctime)s %(levelname)s %(name)s: %(message)s')


def fetch_status_classify(status_code:int) -> str:
    if status_code == 200:
        return "success"
    if status_code == 403:
        return "forbidden"
    if ((status_code >= 500 and status_code <= 599)
        or status_code == 429):
        # server error or "Too many requests"
        return "defer_visits"
    if status_code >= 300 and status_code < 400:
        return "redirect"
    if (status_code == 404 or status_code == 410):
        return "notfound"
    if status_code == 400:
        # bad request
        return "notfound"
    if status_code == 401:
        return "unauthorized"
    return "other"

def write_robotstxt_ranked_list(crawl, args):
    # read robots.txt capture locations from S3
    # - put there by the script get-robotstxt-captures-athena.py
    # - only required columns
    # - filter on crawl
    df = pd.read_parquet(args.s3_robotstxt_table_location,
                         columns=['host', 'domain', 'rank', 'url',
                                  'fetch_status', 'fetch_redirect',
                                  'content_mime_type', 'content_mime_detected'],
                         filters=[('crawl', '==', crawl)])
    count_fetch_success = df.shape[0]
    logging.info('%i robots.txt captures for crawl %s',
                 count_fetch_success, crawl)

    # classify fetch status
    df['robotstxt_fetch_status'] = df['fetch_status'].apply(fetch_status_classify)
    logging.info('Fetch status classification of robots.txt captures:\n%s',
                 df['robotstxt_fetch_status'].value_counts())
    logging.info('Fetch status classified as "other":\n%s',
                 df[df['robotstxt_fetch_status'] == 'other']['fetch_status'].value_counts())

    # classify MIME types
    df['is_robotstxt_mime_type'] = is_robotstxt_mime_type(df)
    logging.info('MIME type classification of robots.txt captures:\n%s',
                 df['is_robotstxt_mime_type'].value_counts())

    # deduplicate
    # (note: in the table with robots.txt captures there are duplicates
    #        because we need to follow all redirects)
    n_rows = df.shape[0]
    df = df.drop_duplicates()
    if df.shape[0] < n_rows:
        logging.info('Removed %d duplicates in ranked list', (n_rows - df.shape[0]))

    # save file
    os.makedirs(os.path.join(args.output_location, 'crawl=' + crawl), exist_ok=True)
    output_path = os.path.join(args.output_location,
                               'crawl=' + crawl,
                               'robotstxt-captures-' + crawl + '.zstd.parquet')
    tschema = pa.schema([
        pa.field('host',                    pa.string()),
        pa.field('domain',                  pa.string()),
        pa.field('rank',                    pa.int32()),
        pa.field('url',                     pa.string()),
        pa.field('fetch_status',            pa.int32()),
        pa.field('fetch_redirect',          pa.string()),
        pa.field('content_mime_type',       pa.string()),
        pa.field('content_mime_detected',   pa.string()),
        pa.field('robotstxt_fetch_status',  pa.string()),
        pa.field('is_robotstxt_mime_type',  pa.bool_())
    ])
    table = pa.Table.from_pandas(df, preserve_index=False, schema=tschema)
    pq.write_table(table, output_path, compression='zstd', compression_level=19)
    logging.info('Ranked list of robots.txt captures saved to %s', output_path)



if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('s3_robotstxt_table_location',
                        help='Location of the robots.txt capture table on S3'
                        ' (or a local copy of it)')
    parser.add_argument('output_location',
                        help='Output location (local directory)')
    parser.add_argument('crawl_data_set', nargs='+',
                        help='Common Crawl crawl dataset(s) to process, eg. CC-MAIN-2022-33')
    args = parser.parse_args()


    for crawl in args.crawl_data_set:
        write_robotstxt_ranked_list(crawl, args)
