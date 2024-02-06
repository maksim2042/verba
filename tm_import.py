import requests
import json
import base64
import os
from io import StringIO
import pandas as pd
import xmltodict
import json
from zipfile import ZipFile
from bs4 import BeautifulSoup
from io import BytesIO
from urllib.request import urlopen


uspto_base_url = 'https://bulkdata.uspto.gov/data/trademark/dailyxml/applications/'
url = "https://idyllic.ngrok.io/api/load_data"
get_url = "https://idyllic.ngrok.io/api/get_all_documents"
delete_url = 'https://idyllic.ngrok.io/api/delete_document'
query_url = 'https://idyllic.ngrok.io/api/query'
gen_url = 'https://idyllic.ngrok.io/api/generate'
data_dir = "raw_data"


codes = open('live_tm_codes.txt').read().split('\n\n')
live_codes = [int(l.split(' ')[0]) for l in codes if 'Live' in l]



def get_all_documents():
	get_payload = {
	  "query": "",
	  "doc_type": "TM"
	}
	docs = requests.post(get_url, data=json.dumps(get_payload))
	docs = json.loads(res.content)
	return(docs['documents'])


def delete_all(docs):
	ids = [d['_additional']['id'] for d in docs]

	for id in ids:

		delete_payload = {
		  "document_id": id
		}
		requests.post(delete_url,data=json.dumps(delete_payload))


def extract_href_links(html):
    soup = BeautifulSoup(html, 'html.parser')
    href_links = [a['href'] for a in soup.find_all('a', href=True)]
    return href_links

def split_records(filename, keyword='<case-file>'):
    current_record = []
    resp = urlopen(uspto_base_url+'/'+filename)
    try:
      with ZipFile(BytesIO(resp.read())) as zip:
        fc = filename.replace('.zip','.xml')
  #    with ZipFile(data_dir+filename, 'r') as zip:
      # printing all the contents of the zip file
        with zip.open(fc) as file:
          for line in file:
              stline = string_data = line.decode('utf-8')
              if keyword in stline:
                  if current_record:
                      yield ''.join(current_record)
                      current_record = []
              current_record.append(stline)

        if current_record:
            yield ''.join(current_record)
    except:
        raise(Exception("couldn't read file" + filename))


def parser(filename):
	error_log = open(data_dir+'errors.xml','a')
	errors = []
	for record in split_records(filename):
		### skip the XML header
		if '<?xml' in record: continue

		## try to parse an XML record
		try:
			rr = xmltodict.parse(record)
		except:
			print("CAN'T PARSE XML", record)
			error_log.write(record)
			continue

		r = rr['case-file']
		try:
			row = {
				'serial-number': r['serial-number'],
				'mark':r['case-file-header']['mark-identification'],
				'status':r['case-file-header']['status-code']
			}

		except:
			errors.append(["No serial, or no TM, skipping", r])
			continue

		print(row['mark'])

		try:
			st = r['case-file-statements']['case-file-statement']
			if type(st) == dict:
				statements = st
			else:
				statements = {s['type-code']:s['text'] for s in st}
				statements_txt = ' '.join(statements.values())
				row['statements'] = statements
		except:
			pass ### if there are no case file statements, just return TM and serial number

		yield(row)


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

#### get the zip file URLs
html = requests.get(uspto_base_url).content

# Extract href links from the HTML file
files = [href for href in extract_href_links(html) if '.zip' in href]
files.reverse() ### do the newest first
ingest_log = []

pd.DataFrame(files).to_csv(data_dir+'raw_files.csv')

for f in files:
	if f in ingest_log: 
		continue ### don't ingest twice

	print(f)
	for tm in parser(f):
		if tm['status'] in live_codes:
			import_payload['fileBytes'].append(base64.b64encode(txt.encode('utf-8')).decode('ascii'))
			import_payload['fileNames'].append(tm['mark'])

	print("found %s live marks")%len(import_payload['fileNames'])

	try:
		res = requests.post(url, data=json.dumps(payload))
	except:
		print("POST failed") ### is the API down?
		break

	if res.status_code != 200:
		print("POST failed with status code %s")%res.status_code
		break

	print("ingest complete, moving on")
	ingest_log.append(f)
	break


#filename = 'apc18840407-20221231-78.zip'
