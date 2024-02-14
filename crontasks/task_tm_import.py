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
trademarks_storage_file = os.path.join(data_dir, 'trademarks_storage.json')
db_connection_string = 'sqlite:////home/stephen/db1.db'
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
storage = models.TrademarkStorage(trademarks_storage_file)
storage.load()

# for f in ['apc18840407-20231231-81.zip']:#files[:1]:
# for f in ['apc18840407-20231231-80.zip']:#files[:1]:
# for f in files[:1]:
for f in files[44:]:
	logger.info(f"\nProcessing file {f}:")
	if f in processed_files:
		continue ### don't ingest twice


	for tm in utils.parser(uspto_base_url, f):
		if tm['status'] in live_codes:
			storage.update(tm)
		else:
			storage.remove(tm)
		print('.', end='')
	print()
	storage.dump()
	utils.add_processed_file(processed_files_file, processed_files, f)


			# import_payload['fileBytes'].append(base64.b64encode(txt.encode('utf-8')).decode('ascii'))
			# import_payload['fileNames'].append(tm['mark'] + '.json')



	# try:
	# 	res = requests.post(url, data=json.dumps(payload))
	# except:
	# 	print("POST failed") ### is the API down?
	# 	break

	# if res.status_code != 200:
	# 	print("POST failed with status code %s" %res.status_code)
	# 	break
	#
	# print("ingest complete, moving on")
	# ingest_log.append(f)
	# break


#filename = 'apc18840407-20221231-78.zip'
