import base64
import json
import logging

from multiprocessing.dummy import Pool
import requests

UPLOAD_DATA_URL = "https://idyllic.ngrok.io/api/load_data"
GET_ALL_DOCS_URL = "https://idyllic.ngrok.io/api/get_all_documents"
SEARCH_DOCS_URL = "https://idyllic.ngrok.io/api/search_documents"
DELETE_DOC_URL = 'https://idyllic.ngrok.io/api/delete_document'
DELETE_MANY_DOCS_URL = 'https://idyllic.ngrok.io/api/delete_many_documents'
QUERY_URL = 'https://idyllic.ngrok.io/api/query'
GEN_URL = 'https://idyllic.ngrok.io/api/generate'

logger = logging.getLogger(__name__)


def send_to_RAG(data_to_send, document_type, filename_generator_func):
    payload = {
        'reader': 'SimpleReader',
        'chunker': 'TokenChunker',
        'embedder': 'MiniLMEmbedder',
        'fileBytes': [],
        'fileNames': [],
        'filePath': '',
        'document_type': document_type,
        'chunkUnits': 100,
        'chunkOverlap': 25,
    }
    for doc in data_to_send:
        txt = json.dumps(doc)
        payload['fileBytes'].append(base64.b64encode(txt.encode('utf-8')).decode('ascii'))
        payload['fileNames'].append(f'{filename_generator_func(doc)}')

    res = requests.post(UPLOAD_DATA_URL, data=json.dumps(payload))
    res.raise_for_status()
    logger.info(f'Data sent with status: {res.status_code}, text: {res.text}')
    status = res.json()['status']
    if status != 200:
        raise requests.HTTPError(f"Verba response: {res.status_code}, text: {res.text}")


def search_documents(filename_query, doc_type, only_ids=False):
    """
    Leave 'filename_query' empty to find all of this type.
    Leave 'doc_type' empty to find any type.
    """
    get_payload = {
        "query": filename_query,
        "doc_type": doc_type
    }
    if filename_query:
        res = requests.post(SEARCH_DOCS_URL, data=json.dumps(get_payload))
    else:
        res = requests.post(GET_ALL_DOCS_URL, data=json.dumps(get_payload))
    res.raise_for_status()
    docs = json.loads(res.content)
    if only_ids:
        return [doc['_additional']['id'] for doc in docs['documents']]
    return docs['documents']


def delete_documents(ids):
    res = requests.post(DELETE_MANY_DOCS_URL, json={"document_ids": ids})
    res.raise_for_status()
    logger.info(f"Deletion of {len(ids)} docs from Verba finished successfully")


    # def thread_func(id):
    #     res = requests.post(DELETE_DOC_URL, json={"document_id": id})
    #     res.raise_for_status()
    #     logger.info(f"Deletion of {id} from Verba finished successfully")
    #
    # with Pool(10) as pool:
    #     pool.map(thread_func, ids)

def delete_by_filename_query(filename_query, doc_type):
    ids = search_documents(filename_query, doc_type, only_ids=True)
    delete_documents(ids)


def delete_all_documents():
    ids = search_documents("", "", only_ids=True)
    delete_documents(ids)