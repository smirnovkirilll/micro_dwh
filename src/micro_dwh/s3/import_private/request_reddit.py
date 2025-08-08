#!/bin/env python
# note: request reddit API, store result to s3, postgresql
# used docs:
# https://www.reddit.com/prefs/apps/
# https://www.reddit.com/dev/api
# https://github.com/reddit-archive/reddit/wiki/OAuth2-Quick-Start-Example


import bz2
import json
import logging
import os
import requests
import requests.auth
import time
from table_transfer.helpers import upload_object_to_s3, timestamp_to_dttm


LISTING = 'new'  # controversial, best, hot, new, random, rising, top
LIMIT = 100
TIMEFRAME = 'month'  # hour, day, week, month, year, all
SUBREDDIT_LIST = os.environ.get('SUBREDDIT_LIST', '').split(',')


logger = logging.getLogger('logging request_reddit')
logger.setLevel(logging.INFO)


def get_reddit_response_no_auth(
        subreddit: str,
        listing: str,
        limit: int,
        timeframe: str,
        before: str = None,
        after: str = None,
) -> dict:
    """
    - simplest request, if breaks => add authorisation
    - to paginate in the past, use argument `after` and use according attribute of response
      (it is equal to the name of minimum/the most early thing at the current page)
    """

    start = time.time()
    base_url = f'https://www.reddit.com/r/{subreddit}/{listing}.json?limit={limit}&t={timeframe}'
    if before and after:
        raise ValueError(f'Specify either `before`={before} argument or `after`={after}, not both')
    elif before:
        base_url = base_url + f'&before={before}'
    elif after:
        base_url = base_url + f'&after={after}'
    else:
        pass

    request = requests.get(base_url, headers={'User-agent': 'my-script'})
    if request.status_code != 200:
        logger.error(f'An error occ: status_code={request.status_code}, reason={request.reason}')
        # note: not raises in runtime, have not found out reason yet
        raise Exception(f'An error occured: status_code={request.status_code}, reason={request.reason}')
    try:
        response = request.json()
        response['_subreddit'] = subreddit
        response['_request_start_time'] = start
        response['_request_end_time'] = time.time()
        logger.info('OK - got reddit response')
        return response
    except Exception as exc:
        logger.error(f'An error occured: {exc}')


def add_metadata(response: dict) -> dict:

    response['_request_duration_sec'] = response['_request_end_time'] - response['_request_start_time']
    response['_max_thing_name'] = response['data']['children'][0]['data']['name']
    response['_max_thing_utc_created'] = response['data']['children'][0]['data']['created_utc']
    response['_max_thing_utc_created_dttm'] = timestamp_to_dttm(response['_max_thing_utc_created'])
    response['_min_thing_name'] = response['data']['children'][-1]['data']['name']
    response['_min_thing_utc_created'] = response['data']['children'][-1]['data']['created_utc']
    response['_min_thing_utc_created_dttm'] = timestamp_to_dttm(response['_min_thing_utc_created'])
    response['_things_given_cnt'] = len(response['data']['children'])
    return response


def _prepare_file_name(
        response: dict,
        add_request_time: bool = False,
        compress: bool = False,
        s3: bool = False,
        hist: bool = False,
) -> str:
    """
    for s3 storage crucial to save subreddit in separate kinda-directory, that's why using /
    for locally storage - just add subreddit as part of filename
    """

    if hist:
        dir_name = response['_subreddit'] + '_hist'
    else:
        dir_name = response['_subreddit']

    if s3:
        suffix_separator = '/'
    else:
        suffix_separator = '__'

    if add_request_time:
        request_time = '__' + timestamp_to_dttm(response['_request_start_time'])
    else:
        request_time = ''

    if compress:
        postfix = '.bz2'
    else:
        postfix = ''

    return dir_name + suffix_separator + \
        response['_min_thing_utc_created_dttm'] + '__' + response['_min_thing_name'] + '__' + \
        response['_max_thing_utc_created_dttm'] + '__' + response['_max_thing_name'] + \
        request_time + '.json' + postfix


def _get_data_obj(some_dict: dict, compress: bool = True) -> bytes:
    # TODO: move to table_transfer.helpers

    data = json.dumps(some_dict).encode('utf-8')
    if compress:
        data = bz2.compress(data)

    return data


def save_dict_locally(
        response: dict,
        add_request_time: bool = True,
        compress: bool = True,
        hist: bool = False,
) -> str:
    # TODO: replace with TableTransfer

    file_name = _prepare_file_name(response, add_request_time, compress=compress, hist=hist)
    data = _get_data_obj(response, compress)
    with open(file_name, 'wb') as file_handle:
        file_handle.write(data)
        logger.info(f'OK - compressed and saved file locally: {file_name}')

    return file_name


def request_and_save_response(
        subreddit: str = None,
        listing: str = LISTING,
        limit: int = LIMIT,
        timeframe: str = TIMEFRAME,
        compress=True,
        s3=True,
        hist=False,
) -> str:
    # TODO: replace with TableTransfer

    response = get_reddit_response_no_auth(subreddit, listing, limit, timeframe)
    full_response = add_metadata(response)
    save_func = upload_dict_to_s3 if s3 else save_dict_locally

    return save_func(full_response, compress=compress, hist=hist)


def upload_dict_to_s3(
        response: dict,
        add_request_time: bool = True,
        compress: bool = True,
        hist=False,
) -> str:
    # TODO: replace with TableTransfer

    bucket = os.environ['S3_BUCKET']
    file_name = _prepare_file_name(response, add_request_time, compress=compress, s3=True, hist=hist)
    data_obj = _get_data_obj(response, compress)
    upload_object_to_s3(bucket, file_name, data_obj)

    return file_name


def main():
    logging.basicConfig(level=logging.INFO)
    for subreddit in SUBREDDIT_LIST:
        request_and_save_response(subreddit)


if __name__ == '__main__':
    main()
