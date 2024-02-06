import requests
import json
import base64
import os
from io import StringIO
import pandas as pd

url = "https://idyllic.ngrok.io/api/load_data"
get_url = "https://idyllic.ngrok.io/api/get_all_documents"
delete_url = 'https://idyllic.ngrok.io/api/delete_document'
query_url = 'https://idyllic.ngrok.io/api/query'
gen_url = 'https://idyllic.ngrok.io/api/generate'
data_dir = "raw_data"

start_dir = 'NCT0618xxxx'



def get_all_documents():
	get_payload = {
	  "query": "",
	  "doc_type": "CT"
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

query_payload = {
  "query": "What clinical trials do you have for use of exercise to prevent heart attacks?"
}
requests.post(query_url,data=json.dumps(query_payload))

{'query': 'What clinical trials do you have for use of exercise to prevent heart attacks?',
 'context': "result of retrieved documents goes here",
 'conversation': [
 		{'type': 'user', 'content':'string', 'typewriter':'false'},
 		{'type': 'system', 'content':'string', 'typewriter':'false'}
 ]
}





def json2txt(js):
	idd = js['FullStudy']['Study']['ProtocolSection']['IdentificationModule']
	idd_txt = idd.get('NCTId','')+idd.get('BriefTitle','')+"\n"

	desc = js['FullStudy']['Study']['ProtocolSection']['DescriptionModule']
	desc_txt = "DESCRIPTION >>>" + desc.get('BriefSummary','') + desc.get('DetailedDescription','')+"\n"

	cond = js['FullStudy']['Study']['ProtocolSection']['ConditionsModule']+"\n"
	#cond_txt = ','.join(cond['ConditionList'].get('Condition'))

	design = js['FullStudy']['Study']['ProtocolSection']['DesignModule']+"\n"

	eligibility = js['FullStudy']['Study']['ProtocolSection']['EligibilityModule']+"\n"
	#eligibility_txt = "ELIGIBILIUTY >>>" + eligibility.get('EligibilityCriteria','')

	return(idd_txt + desc_txt + json.dumps(cond)+ json.dumps(eligibility)+json.dumps(design))


payload = {'reader': 'SimpleReader',
	 'chunker': 'TokenChunker',
	 'embedder': 'MiniLMEmbedder',
	 'fileBytes': [],
	 'fileNames': [],
	 'filePath': '',
	 'document_type': 'CT',
	 'chunkUnits': 100,
	 'chunkOverlap': 25,
	}

# that directory
for main_dir in os.listdir(data_dir):
	directory = data_dir+"/"+main_dir
	print("INGESTING", main_dir)	
	for filename in os.listdir(directory):
		f = os.path.join(directory, filename)
		# checking if it is a file
		if os.path.isfile(f):
			print(f)
			data = json.load(open(f))
			try:
				txt = json2txt(data)
			except:
				print(":-(")

			filename = filename.split('.')[0]+".txt"
	#		payload['fileBytes'].append(base64.b64encode(json.dumps(data).encode('utf-8')).decode('ascii'))
			payload['fileBytes'].append(base64.b64encode(txt.encode('utf-8')).decode('ascii'))
			payload['fileNames'].append(filename)

	res =requests.post(url, data=json.dumps(payload))
	if res.status_code == 200:
		print("SUCCESS")
	else:
		print(":-(", res.status_code)