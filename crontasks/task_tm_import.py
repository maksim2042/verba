import argparse
import logging
import datetime as dt
import logging
import os
import sys
from enum import Enum

import requests
from sqlalchemy.orm import Session

from crontasks.tm_import import models
from crontasks.tm_import import verba
from crontasks.tm_import import utils

class PARSER_MODES(Enum):
    DATA_COLLECTION = 'data-collection'
    INITIAL_SENDING = 'initial-sending'
    TEST_GET_ALL_DOCUMENTS = 'test-get-all-docs'
    TEST_DELETE_ALL_DOCUMENTS = 'test-del-all-docs'

SCRIPT_DIR = os.path.dirname(__file__)
USPTO_BASE_URL = 'https://bulkdata.uspto.gov/data/trademark/dailyxml/applications/'
DATA_DIR = os.path.join(SCRIPT_DIR, "tm_import/")
LIVE_TM_CODES_FILEPATH = os.path.join(DATA_DIR, 'live_tm_codes_from_markavo_com.txt')
PROCESSED_FILES_FILEPATH = os.path.join(DATA_DIR, 'processed_files.txt')
DB_CONNECTION_STRING = 'sqlite:///' + os.path.join(DATA_DIR, 'trademarks_DB.sqlite')
LOG_FILEPATH = os.path.join(DATA_DIR, 'logs/tm_import.log')
RECORDS_PER_FILE_TO_SEND = 10
FILES_PER_REQUEST_TO_SEND = 10


def thread_processing(data):
    filename = data
    log_prefix = f"{filename}: "
    if filename in processed_files:
        logger.info(f"{log_prefix}Skipping {filename}...")
        return
    logger.info(f"{log_prefix}Starting to process file {filename}:")
    time1 = dt.datetime.now()
    i = -1
    with Session(storage._engine) as session:
        for i, tm in enumerate(utils.parser(USPTO_BASE_URL, filename)):
            storage.add_trademark(tm, session)
            if (i + 1) % 10000 == 0:
                session.commit()
                logger.info(f"{log_prefix}{i + 1} done ({dt.datetime.now() - time1} have passed)")
        session.commit()
    utils.add_processed_file(PROCESSED_FILES_FILEPATH, filename)
    logger.info(f"{log_prefix}finished successfully (upserted {i+1} records)")


try:
    os.makedirs(os.path.dirname(LOG_FILEPATH), exist_ok=True)
    logging.basicConfig(filename=LOG_FILEPATH, encoding='utf-8', level=logging.INFO)
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(prog='Trademarks parser')
    parser.add_argument('-m', '--mode', choices=[e.value for e in PARSER_MODES])
    parser.add_argument('-d', '--db_cstring')
    args = parser.parse_args()
    mode=PARSER_MODES(args.mode)
    db_cstring = args.db_cstring if args.db_cstring else  DB_CONNECTION_STRING

    os.makedirs(DATA_DIR, exist_ok=True)

    logger.info(f"Trademarks importer script has been started (mode={mode})")
    live_codes = utils.read_live_tm_codes(LIVE_TM_CODES_FILEPATH)
    storage = models.TrademarkStorage(db_cstring, live_codes)

    if mode == PARSER_MODES.DATA_COLLECTION:
        html = requests.get(USPTO_BASE_URL).content
        # Extract href links from the HTML file
        files = [href for href in utils.extract_href_links(html) if '.zip' in href]
        processed_files = utils.read_processed_files(PROCESSED_FILES_FILEPATH)
        files = sorted(set(files) - set(processed_files), reverse=True)
        # for f in ['apc18840407-20231231-04.zip']:
        for f in files:
            thread_processing(f)

    elif mode == PARSER_MODES.INITIAL_SENDING:
        data_to_send = []
        for i, data in enumerate(storage.fetch_data_for_RAG()):
            data_to_send.append(data)
            if (i + 1) % RECORDS_PER_FILE_TO_SEND == 0:
                verba.send_to_RAG(data_to_send, 'Trademark', lambda x: f"{x['serial-number']} {x['trademark-name']}")
                data_to_send.clear()
                logger.info(f"{i} records have been sent to Verba")

    elif mode == PARSER_MODES.TEST_GET_ALL_DOCUMENTS:
        print(verba.search_documents('JUDITH', 'Trademark'))

    elif mode == PARSER_MODES.TEST_DELETE_ALL_DOCUMENTS:
        verba.delete_by_filename_query('71073603', 'Trademark')






except:
    logger.exception('')
    raise
