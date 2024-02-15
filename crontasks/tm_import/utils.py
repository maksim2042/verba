import traceback
from zipfile import ZipFile
from bs4 import BeautifulSoup
from io import BytesIO
from urllib.request import urlopen
import xmltodict



# def get_all_documents():
# 	get_payload = {
# 	  "query": "",
# 	  "doc_type": "TM"
# 	}
# 	docs = requests.post(get_url, data=json.dumps(get_payload))
# 	docs = json.loads(res.content)
# 	return(docs['documents'])
#
#
# def delete_all(docs):
# 	ids = [d['_additional']['id'] for d in docs]
#
# 	for id in ids:
#
# 		delete_payload = {
# 		  "document_id": id
# 		}
# 		requests.post(delete_url,data=json.dumps(delete_payload))


def extract_href_links(html):
    soup = BeautifulSoup(html, 'html.parser')
    href_links = [a['href'] for a in soup.find_all('a', href=True)]
    return href_links

def split_records(uspto_base_url, filename, keyword='<case-file>'):
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
    except Exception as e:
        raise(Exception("couldn't read file" + filename))


def parser(uspto_base_url, filename):
	errors = []
	for record in split_records(uspto_base_url, filename):
		### skip the XML header
		if '<?xml' in record: continue

		## try to parse an XML record
		try:
			rr = xmltodict.parse(record)
		except:
			# error_log.write(f"Can't parse xml:\n{traceback.format_exc()}")
			# error_log.flush()
			# continue
			print()

		r = rr['case-file']
		if not(type(r['transaction-date']) is str and len(r['transaction-date']) == 8):
			print() # TODO: a place for a debugger breakpoint (an unusual case for analysis)
		try:
			row = {
				'serial-number': r['serial-number'],
				'mark':r['case-file-header']['mark-identification'],
				'status': int(r['case-file-header']['status-code']),
				'transaction-date': r['transaction-date'],
				'statements': _normalize_statements(r.get('case-file-statements')),
			}

		except:
			errors.append(["No serial, or no TM, skipping", r])
			continue

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


def read_live_tm_codes(live_tm_codes_file):
	with open(live_tm_codes_file) as file:
		lines = [line.rstrip() for line in file]
	live_codes = [int(l.split(' ')[0]) for l in lines if 'Live' in l]
	return live_codes


def read_processed_files(processed_files_file):
	try:
		with open(processed_files_file) as file:
			return [line.strip() for line in file]
	except FileNotFoundError:
		return []


def add_processed_file(processed_files_file, processed_files, new_processed_file):
	with open(processed_files_file, 'a') as file:
		file.write(new_processed_file + '\n')
	processed_files.append(new_processed_file)

def _normalize_statements(statements):
	if statements is None:
		return {}
	if type(statements['case-file-statement']) is dict:
		return {
			statements['case-file-statement']['type-code']: statements['case-file-statement']['text']
		}
	else:
		return statements