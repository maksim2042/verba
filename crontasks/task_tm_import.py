import argparse
import datetime as dt
import logging
import os
import sys
import time
from enum import Enum

import requests
from dotenv import load_dotenv
from sqlalchemy.orm import Session

from .tm_import import models
from .tm_import import utils
from .tm_import.verba_native import VerbaNativeAdapter
from .tm_import.weaviate import get_weaviate_client, get_or_create_collection

load_dotenv()

class PARSER_MODES(Enum):
    DATA_COLLECTING = 'data-collecting'
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
DB_CLASS_NAME = 'trademarks_test_01'


def process_data_from_file(data):
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


def _send_to_verba_with_retries(docs_to_send, names_of_docs_to_send):
    for attempt in range(10):
        try:
            docs, chunks = verba.upload_documents("Trademark", docs_to_send, names_of_docs_to_send, 5000, 0)
            if docs < RECORDS_PER_REQUEST_TO_SEND:
                logger.error(f"Inserted {docs} documents instead of {RECORDS_PER_REQUEST_TO_SEND}. Exiting")
                exit(-3)
            break
        except Exception as e:
            logger.warning(f"Couldn't send data to Weaviate (was attempt {attempt + 1}/10)", exc_info=True)
            if attempt == 9:
                raise
            time.sleep(10)

def _upload_to_weaviate(collection, docs):
    res = collection.data.insert_many(docs)
    if res.errors or len(res.uuids) < RECORDS_PER_REQUEST_TO_SEND:
        print()
    return res.uuids.values()



try:
    os.makedirs(os.path.dirname(LOG_FILEPATH), exist_ok=True)
    log_format = '%(asctime)s %(levelname)-8s {%(pathname)s:%(lineno)d}:\n%(message)s'
    logging.basicConfig(
        filename=LOG_FILEPATH,
        encoding='utf-8',
        level=logging.INFO,
        format=log_format,
    )
    std_handler = logging.StreamHandler(sys.stdout)
    std_handler.setFormatter(logging.Formatter(log_format))
    logging.getLogger().addHandler(std_handler)

    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(prog='Trademarks parser')
    parser.add_argument('-m', '--mode', choices=[e.value for e in PARSER_MODES])
    parser.add_argument('-d', '--db_cstring')
    parser.add_argument('--ismode_del_all', default='False')
    args = parser.parse_args()
    mode=PARSER_MODES(args.mode)
    db_cstring = args.db_cstring if args.db_cstring else os.getenv('TRADEMARKS_DB_CSTRING')

    os.makedirs(DATA_DIR, exist_ok=True)

    logger.info(f"Trademarks importer script has been started (mode={mode})")
    live_codes = utils.read_live_tm_codes(LIVE_TM_CODES_FILEPATH)

    storage = models.TrademarkStorage(db_cstring, live_codes)
    # verba = VerbaNativeAdapter()
    wv_client = get_weaviate_client()

    if mode == PARSER_MODES.INITIAL_SENDING:
        if args.ismode_del_all.lower() == 'true':
            # todo
            raise NotImplementedError("delete_documents_by() doesn't support full db deletion")
            # verba.delete_documents_by('Trademark', '')
            # docs_exist = []
        else:
            pass
            # todo
            # documents = verba.retrieve_all_documents()
            # docs_exist = [models.TrademarkData.extract_serial_from_docname(i1['doc_name']) for i1 in documents if i1['doc_type'] == 'Trademark']
        # docs_exist = []
        wv_client.collections.delete(DB_CLASS_NAME)
        collection = get_or_create_collection(DB_CLASS_NAME)

        docs_to_send = []
        all_uploaded_docs = []
        names_of_docs_to_send = []
        i = 0
        for data in storage.fetch_trademarks_data_from_db(limit=10):
            # if data.serial_number in docs_exist: #TODO
            #     continue
            as_dict = data.as_dict()
            docs_to_send.append(as_dict)
            all_uploaded_docs.append(as_dict)
            i += 1
            if i % RECORDS_PER_REQUEST_TO_SEND == 0:
                _upload_to_weaviate(collection, docs_to_send)
                docs_to_send.clear()
                logger.info(f"Total {i} records processed")
                time.sleep(20)

        if docs_to_send:
            _upload_to_weaviate(collection, docs_to_send)
        logger.info(f"Finished. {i} records total have been inserted to Weaviate")
        print(all_uploaded_docs)
        # TODO TAKE ALL_UPLOADED_DOCS

    elif mode == PARSER_MODES.DATA_COLLECTING:
        existed_docs = verba.retrieve_all_documents(doc_type="Trademark")
        serial_id_mapping = utils.build_serial_id_mapping(existed_docs)

        processed_files = utils.read_processed_files(PROCESSED_FILES_FILEPATH)
        files = sorted(set(processed_files), reverse=True)
        file_dates = [utils.parse_date_string(f[3:-4]) for f in files]
        for data in storage.fetch_trademarks_data_from_db(file_dates):
            ...
        exit(0)


        html = requests.get(USPTO_BASE_URL).content
        # Extract href links from the HTML file
        files = [href for href in utils.extract_href_links(html) if '.zip' in href]
        processed_files = utils.read_processed_files(PROCESSED_FILES_FILEPATH)
        files = sorted(set(files) - set(processed_files), reverse=True)
        file_dates = [utils.parse_date_string(f[3:-4]) for f in files]
        for f in files:
            process_data_from_file(f)
        for data in storage.fetch_trademarks_data_from_db(file_dates):
            ...

    elif mode == PARSER_MODES.TEST_GET_ALL_DOCUMENTS:
        ...

    elif mode == PARSER_MODES.TEST_DELETE_ALL_DOCUMENTS:
        ...


except:
    logger.exception('')
    raise
