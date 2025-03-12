"""Extract download list of WARC records from the result of
`get_robotstxt_captures_athena.py`.
"""

import argparse
import os
import logging

import pandas as pd


logging.basicConfig(level='INFO',
                    format='%(asctime)s %(levelname)s %(name)s: %(message)s')


def is_robotstxt_mime_type(df:pd.DataFrame) -> pd.Series:
    """Returns boolean vector (pandas Series) indicating which rows
    are robots.txt MIME types or not. The columns `content_mime_type`
    and `content_mime_detected` are expected in the DataFrame.
    """
    # - note: the column 'content_mime_detected' is populated since CC-MAIN-2018-34.
    #         If it's missing (null), need to fall back to the noisy MIME type
    #         sent in the HTTP Content-Type header.
    # 1. if 'content_mime_detected' is defined, only keep detected MIME types
    #    - starting with 'text/'
    #    - but not 'text/html'
    #    - also allow 'message/rfc822' (misdetection) or 'plain/text'
    s1 = (df['content_mime_detected'].isna()
          | ((df['content_mime_detected'].str.startswith('text/')
              & ~(df['content_mime_detected'] == 'text/html'))
             | (df['content_mime_detected'] == 'message/rfc822')
             | (df['content_mime_detected'] == 'plain/text')))
    # 2. filter on 'content_type' if 'content_mime_detected' is null
    #    - simply check whether it contains the string 'text'
    #    - but not 'text/html'
    s2 = ((~df['content_mime_detected'].isna())
          | df['content_mime_type'].isna()
          | df['content_mime_type'].str.contains('(?i)text(?!/html)', case=False, regex=True))
    return s1 & s2

def write_robotstxt_download_list(crawl, args):
    # read robots.txt capture locations from S3
    # - put there by the script get_robotstxt_captures_athena.py
    # - only required columns
    # - filter on crawl
    # - filter only successful fetches
    # - and text/plain MIME types (and equivalent), excluding HTML
    df = pd.read_parquet(args.s3_robotstxt_table_location,
                         columns=['url', 'warc_filename', 'warc_record_offset', 'warc_record_length',
                                  'content_mime_type', 'content_mime_detected'],
                         filters=[('crawl', '==', crawl),
                                  ('fetch_status', '==', 200)])
    count_fetch_success = df.shape[0]
    logging.info('Extracted %i successfully fetched robots.txt captures for crawl %s',
                 count_fetch_success, crawl)

    # filter MIME types
    df = df[is_robotstxt_mime_type(df)]
    logging.info('After filtering by MIME type, got %i robots.txt captures for crawl %s',
                 df.shape[0], crawl)

    # strip unneeded columns
    df = df[['url', 'warc_filename', 'warc_record_offset', 'warc_record_length']]

    # deduplicate
    # (note: in the table with robots.txt captures there are duplicates
    #        because we need to follow all redirects)
    n_rows = df.shape[0]
    df = df.drop_duplicates()
    if df.shape[0] < n_rows:
        logging.info('Removed %d duplicates in download list', (n_rows - df.shape[0]))

    # save file
    os.makedirs(os.path.join(args.output_location, 'crawl=' + crawl), exist_ok=True)
    output_path = os.path.join(args.output_location,
                               'crawl=' + crawl,
                               'robotstxt-captures-' + crawl + '.csv')
    df.to_csv(output_path, header=True, index=False)
    logging.info('Download list saved to %s', output_path)



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
        write_robotstxt_download_list(crawl, args)
