# warning: incorrect way of export: DWH layering dismissed in favor of faster website launch
import logging
import os
from table_transfer import TableTransfer


logger = logging.getLogger('logging initial_curated_list')
logger.setLevel(logging.INFO)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    # local-csv-to-s3-json
    dirty_curated_list = TableTransfer(
        source_file_name=os.environ['LOCAL_SOURCE_CURATED_LIST_CSV'],
        target_s3_bucket=os.environ['S3_BUCKET_PUBLIC_LOOKMOM'],
        target_file_name=os.environ['S3_TARGET_CURATED_LIST_JSON'],
    )
    dirty_curated_list.get_entries_from_csv()
    dirty_curated_list.upload_entries_to_json()
