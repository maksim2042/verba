import argparse
import datetime as dt
import logging
import os
import sys
from enum import Enum

import requests
from dotenv import load_dotenv
from sqlalchemy.orm import Session

from crontasks.tm_import.verba_native import VerbaNativeAdapter
from tm_import import models
from tm_import import verba_api
from tm_import import utils

load_dotenv()

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
# DB_CONNECTION_STRING = 'sqlite:///' + os.path.join(DATA_DIR, 'trademarks_DB3.sqlite')
# DB_CONNECTION_STRING = (
#     'postgresql://'
#     'postgres:cHLa9RzH9QV5rbc@'
#     'idyllic-trademarks.c7ukoiuym4og.us-east-2.rds.amazonaws.com:5432'
#     '/TrademarksDB?sslmode=allow'
# )
LOG_FILEPATH = os.path.join(DATA_DIR, 'logs/tm_import.log')
RECORDS_PER_REQUEST_TO_SEND = 100


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
    db_cstring = args.db_cstring if args.db_cstring else os.getenv('TRADEMARKS_DB_CSTRING')

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
        verba = VerbaNativeAdapter()
        docs_to_send = []
        names_of_docs_to_send = []
        i = -1
        verba.delete_all_by_doctype('')
        for i, data in enumerate(storage.fetch_trademarks_data_from_db()):
            docs_to_send.append(data.as_json_string())
            names_of_docs_to_send.append(data.name_in_verba)
            if (i + 1) % RECORDS_PER_REQUEST_TO_SEND == 0:
                docs, chunks = verba.upload_documents("Trademark", docs_to_send, names_of_docs_to_send)
                docs_to_send.clear()
                names_of_docs_to_send.clear()
                logger.info(f"{docs} docs and {chunks} chunks have been inserted to Verba (total {i+1} records)")
        logger.info(f"Finished. {i+1} records total have been inserted to Verba")

    elif mode == PARSER_MODES.TEST_GET_ALL_DOCUMENTS:
        print(verba_api.search_documents('JUDITH', 'Trademark'))

    elif mode == PARSER_MODES.TEST_DELETE_ALL_DOCUMENTS:
        verba_api.delete_by_filename_query('71073603', 'Trademark')






except:
    logger.exception('')
    raise
