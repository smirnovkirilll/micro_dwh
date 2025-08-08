#!/bin/env python
# note: given export CSV file https://support.mozilla.org/en-US/kb/exporting-your-pocket-list
#   enrich those entries with additional info (locally) and store result to s3 (and postgresql DB - other script)


import logging
import os
import threading
from bs4.builder import ParserRejectedMarkup
from requests import Session
from requests.exceptions import ConnectionError, MissingSchema
from urllib3.exceptions import MaxRetryError, NewConnectionError, NameResolutionError

from table_transfer import (
    is_correct_url,
    secure_url,
    get_domain_by_url,
    get_title_by_url,
    get_unshorten_url,
    make_clean_url,
    date_to_timestamp,
    requests_session,
    timestamp_to_dttm,
    TableTransfer,
)


logger = logging.getLogger('logging initial_pocket_export')
logger.setLevel(logging.INFO)


ExceptionTuple = (
    ConnectionError,
    MissingSchema,
    MaxRetryError,
    NewConnectionError,
    NameResolutionError,
    ParserRejectedMarkup,
    UnicodeDecodeError,
)


def enrich_row(
        row: dict[str: str],
        rename_only: bool = True,
        session: Session = None,
) -> dict[str: str]:
    """enrich pocket entry dict if it does not contain new not-empty keys yet"""

    logger.info(f'Start processing {row=}')

    def _get_unshorten_url(_url, _session):
        try:
            return get_unshorten_url(_url, _session), False
        except ExceptionTuple:
            return _url, True

    def _get_title_by_url(_url, _session, _original_title):
        try:
            return get_title_by_url(_url, _session), False
        except ExceptionTuple:
            return _original_title, True

    if rename_only:
        if row.get('processing_status') in ('RENAMED', 'PROCESSED'):
            enriched_row = row
        else:
            enriched_row = {
                'original_url': row['url'],
                'original_title': row['title'],
                'time_added': row['time_added'],
                'tags': row['tags'],
                'status': row['status'],
                'processing_status': 'RENAMED',
                'clean_url': '',
                'unshorten_url': '',
                'domain_url': '',
                'clean_title': '',
                'utc_added_dttm': '',
                'errors': False,
            }
    else:
        if row.get('processing_status') == 'PROCESSED':
            enriched_row = row
        elif row.get('processing_status') == 'RENAMED':
            clean_url = make_clean_url(row['original_url'])
            unshorten_url, error_uu = _get_unshorten_url(clean_url, session)
            clean_title, error_ct = _get_title_by_url(unshorten_url, session, row['original_title'])
            errors = error_uu or error_ct

            enriched_row = {
                'original_url': row['original_url'],
                'original_title': row['original_title'],
                'time_added': row['time_added'],
                'tags': row['tags'],
                'status': row['status'],

                'clean_url': clean_url,
                'unshorten_url': unshorten_url,
                'domain_url': get_domain_by_url(unshorten_url),
                'clean_title': clean_title,
                'utc_added_dttm': timestamp_to_dttm(int(row['time_added'])),

                'processing_status': 'PROCESSED',
                'errors': errors,
            }
        else:
            clean_url = make_clean_url(row['url'])
            unshorten_url, error_uu = _get_unshorten_url(clean_url, session)
            clean_title, error_ct = _get_title_by_url(unshorten_url, session, row['title'])
            errors = error_uu or error_ct

            enriched_row = {
                'original_url': row['url'],
                'original_title': row['title'],
                'time_added': row['time_added'],
                'tags': row['tags'],
                'status': row['status'],

                'clean_url': clean_url,
                'unshorten_url': unshorten_url,
                'domain_url': get_domain_by_url(unshorten_url),
                'clean_title': clean_title,
                'utc_added_dttm': timestamp_to_dttm(int(row['time_added'])),

                'processing_status': 'PROCESSED',
                'errors': errors,
            }

    # if threading in use https://stackoverflow.com/a/78112476
    threading.current_thread().return_value = enriched_row
    logger.info(f'Finished processing {enriched_row=}')

    return enriched_row


def enrich_group_of_rows(
        list_of_rows: list[dict[str: str]],
        rename_only: bool = True,
        multi_threading: bool = True,
) -> list[dict[str: str]]:
    """multi threading works fine, but it's hard to debug some url requests"""

    threads = []
    enriched_rows = []
    session = requests_session()

    if multi_threading:
        for row in list_of_rows:
            t = threading.Thread(
                target=enrich_row,
                args=(row,),
                kwargs={'rename_only': rename_only, 'session': session},
            )
            threads.append(t)

        for t in threads:
            t.start()

        for t in threads:
            t.join()
            enriched_rows.append(t.return_value)
    else:
        for row in list_of_rows:
            enriched_rows.append(enrich_row(row, rename_only=rename_only, session=session))

    return enriched_rows


def enrich_chunk_of_pocket_export_rows(
        source_file_name,
        min_index=0,
        chunk_size=100,
        multi_threading: bool = True,
) -> None:
    """enrich inplace by chunks of given size"""

    max_index = min_index + chunk_size
    pocket = TableTransfer(
        source_file_name=source_file_name,
        target_file_name=source_file_name,
    )
    pocket.get_entries_from_csv()

    if min_index > 0:
        min_unchanged_part = pocket.list_of_dicts_entries[0: min_index]
    else:
        min_unchanged_part = []

    if max_index >= len(pocket.list_of_dicts_entries):
        max_unchanged_part = []
    else:
        max_unchanged_part = pocket.list_of_dicts_entries[max_index:]

    changed_part = enrich_group_of_rows(
        pocket.list_of_dicts_entries[min_index:max_index],
        rename_only=False,
        multi_threading=multi_threading,
    )
    pocket.list_of_dicts_entries = min_unchanged_part + changed_part + max_unchanged_part
    pocket.upload_entries_to_csv()


def fix_pocket_old_row(
        row: dict[str: str],
) -> dict[str: str]:
    """
    old entries used to be filled manually, some fixes to be applied, idempotent func
    """

    fixed_row = row.copy()

    # 1. check and fix url
    cur_url = fixed_row['url']
    if not is_correct_url(cur_url):
        logger.info(f'Skip processing {row=}')
        return {}
    fixed_row['url'] = secure_url(cur_url)

    # 2. fix status
    status_renaming_map = {
        'ок': 'archive',
    }
    cur_status = fixed_row['status']
    if cur_status in status_renaming_map.values():
        pass
    elif cur_status in status_renaming_map:
        fixed_row['status'] = status_renaming_map[cur_status]
    else:
        fixed_row['status'] = 'unread'

    # 3. fix tags
    tags_renaming_map = {
        'бизнес': 'business',
        'ИТ': 'IT',
        'общество': 'society',
        'подборка': 'selection',
        'прогр.основы': 'software',
        'психология': 'psychology',
        'техника': 'tech',
        'финансы': 'finance',
        'экономика': 'economics',
    }
    cur_tags = fixed_row['tags']
    if cur_tags in tags_renaming_map.values():
        pass
    elif cur_tags in tags_renaming_map:
        fixed_row['tags'] = tags_renaming_map[cur_tags]
    else:
        fixed_row['tags'] = None

    # 4. fix time_added
    cur_date_added = fixed_row['date_added'] or '01.01.1970'
    if '/' in cur_date_added:
        try:
            fixed_row['time_added'] = date_to_timestamp(cur_date_added, '%d/%m/%Y')
        except ValueError:
            fixed_row['time_added'] = date_to_timestamp(cur_date_added, '%m/%d/%Y')
    else:
        fixed_row['time_added'] = date_to_timestamp(cur_date_added, '%d.%m.%Y')

    return fixed_row


def fix_pocket_old_group_of_rows(
        list_of_rows: list[dict[str: str]],
) -> list[dict[str: str]]:

    return [fix_pocket_old_row(row) for row in list_of_rows]


def enrich_pocket_export(
        source_file_name: str,
        target_file_name: str,
        target_s3_bucket: str,
        target_s3_file_name: str,
        fix_pocket_old: bool = False,
        multi_threading: bool = True,
) -> None:
    """to be used locally"""

    # 0. fix old pocket entries and save to the same file (in place)
    if fix_pocket_old:
        pocket = TableTransfer(
            source_file_name=source_file_name,
            target_file_name=source_file_name,
        )
        pocket.get_entries_from_csv()
        pocket.list_of_dicts_entries = fix_pocket_old_group_of_rows(pocket.list_of_dicts_entries)
        pocket.upload_entries_to_csv()

    # 1. rename columns and save to target file (new file)
    pocket = TableTransfer(
        source_file_name=source_file_name,
        target_file_name=target_file_name,
    )
    pocket.get_entries_from_csv()
    pocket.list_of_dicts_entries = enrich_group_of_rows(
        pocket.list_of_dicts_entries, rename_only=True)
    pocket.upload_entries_to_csv()

    # 2. enrich by chunks and save to the same file (in place)
    chunk_size = 500
    min_index_list = [0 + chunk_size * i for i in range(8)]
    for min_index in min_index_list:
        logger.info(f'Start processing {min_index=}')
        enrich_chunk_of_pocket_export_rows(
            source_file_name=target_file_name,
            min_index=min_index,
            chunk_size=chunk_size,
            multi_threading=multi_threading,
        )

    # 3. upload to s3 as csv
    pocket = TableTransfer(
        source_file_name=target_file_name,
        target_s3_bucket=target_s3_bucket,
        target_file_name=target_s3_file_name,
    )
    pocket.get_entries_from_csv()
    pocket.upload_entries_to_csv()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    enrich_pocket_export(
        source_file_name=os.environ['LOCAL_POCKET_EXPORT_SOURCE_FILE_NAME_CSV'],
        target_file_name=os.environ['LOCAL_POCKET_EXPORT_TARGET_FILE_NAME_CSV'],
        target_s3_bucket=os.environ['S3_BUCKET_PRIVATE_DATA_PROCESSING'],
        target_s3_file_name=os.environ['S3_POCKET_EXPORT_TARGET_FILE_NAME_CSV'],
    )

    enrich_pocket_export(
        source_file_name=os.environ['LOCAL_POCKET_EXPORT_OLD_SOURCE_FILE_NAME_CSV'],
        target_file_name=os.environ['LOCAL_POCKET_EXPORT_OLD_TARGET_FILE_NAME_CSV'],
        target_s3_bucket=os.environ['S3_BUCKET_PRIVATE_DATA_PROCESSING'],
        target_s3_file_name=os.environ['S3_POCKET_EXPORT_OLD_TARGET_FILE_NAME_CSV'],
        fix_pocket_old=True,
    )

    # TODO:
    #   fix os.environ names, move PG part to dedicated package
    #   upload_pocket_export_to_postgresql() (cloud)
    #   upload_pocket_export_old_to_postgresql() (cloud)
    # https://www.psycopg.org/docs/cursor.html#cursor.copy_expert
    # https://www.postgresql.org/docs/current/sql-copy.html
    # https://stackoverflow.com/questions/44672524/how-to-create-in-memory-file-object
