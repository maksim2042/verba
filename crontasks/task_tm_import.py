import copy
import logging
import random
import sys

import requests
import traceback
import json
import base64
import os
from io import StringIO
import pandas as pd
import sqlalchemy
import json

from crontasks.tm_import import models
from crontasks.tm_import import utils


script_dir = os.path.dirname(__file__)
uspto_base_url = 'https://bulkdata.uspto.gov/data/trademark/dailyxml/applications/'
url = "https://idyllic.ngrok.io/api/load_data"
get_url = "https://idyllic.ngrok.io/api/get_all_documents"
delete_url = 'https://idyllic.ngrok.io/api/delete_document'
query_url = 'https://idyllic.ngrok.io/api/query'
gen_url = 'https://idyllic.ngrok.io/api/generate'
data_dir = os.path.join(script_dir, "tm_import/")
live_tm_codes_file = os.path.join(data_dir, 'live_tm_codes_from_markavo_com.txt')
processed_files_file = os.path.join(data_dir, 'processed_files.txt')
trademarks_storage_db = 'sqlite:///' + os.path.join(data_dir, 'trademarks_DB.sqlite')
log_filename = os.path.join(data_dir, 'logs/tm_import.log')

os.makedirs(data_dir, exist_ok=True)
os.makedirs(os.path.dirname(log_filename), exist_ok=True)
logging.basicConfig(filename=log_filename, encoding='utf-8', level=logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
logger = logging.getLogger(__name__)

error_log = open(data_dir + 'errors.xml', 'a', buffering=1)

live_codes = utils.read_live_tm_codes(live_tm_codes_file)


# dbEngine = sqlalchemy.create_engine(trademarks_db_connection_string)

#### get the zip file URLs
html = requests.get(uspto_base_url).content

# Extract href links from the HTML file
files = [href for href in utils.extract_href_links(html) if '.zip' in href]
files.reverse() ### do the newest first
processed_files = utils.read_processed_files(processed_files_file)
storage = models.TrademarkStorage(trademarks_storage_db, live_codes)


import_payload = {'reader': 'SimpleReader',
	 'chunker': 'TokenChunker',
	 'embedder': 'MiniLMEmbedder',
	 'fileBytes': [],
	 'fileNames': [],
	 'filePath': '',
	 'document_type': 'CT',
	 'chunkUnits': 100,
	 'chunkOverlap': 25,
	}

# for f in ['apc18840407-20231231-81.zip']:#files[:1]:
# for f in ['apc18840407-20231231-80.zip']:#files[:1]:
for f in files[:1]:
# for f in files[:44]:
	logger.info(f"Processing file {f}:")
	if f in processed_files:
		continue ### don't ingest twice

	for i, tm in enumerate(utils.parser(uspto_base_url, f)):
		storage.add_trademark(tm)
		print('.', end='')
		if (i+1) % 100 == 0:
			logger.info(f"\n{i+1} done")

	utils.add_processed_file(processed_files_file, processed_files, f)

# txt = json.dumps(storage._data)
# storage._data = {}
# payload = copy.deepcopy(import_payload)
# payload['fileBytes'].append(base64.b64encode(txt.encode('utf-8')).decode('ascii'))
# payload['fileNames'].append(tm['mark'] + '.json')
# try:
# 	res = requests.post(url, data=json.dumps(payload))
# 	print(f'\nsent with status: {res.status_code}, text: {res.text}')
# except Exception as e:
# 	print("POST failed") ### is the API down?