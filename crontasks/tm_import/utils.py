import datetime as dt
import logging
import os
import pathlib
import threading
import time
import traceback
import typing
from functools import wraps
from io import BytesIO
from urllib.request import urlopen
from zipfile import ZipFile

import xmltodict
from bs4 import BeautifulSoup


logger = logging.getLogger(__name__)


def extract_href_links(html):
    soup = BeautifulSoup(html, 'html.parser')
    href_links = [a['href'] for a in soup.find_all('a', href=True)]
    return href_links


def split_records(uspto_base_url, filename, keytag='case-file'):
    WAITING, SAVING = range(2)
    state = WAITING

    current_record = []
    time1 = dt.datetime.now()
    resp = urlopen(uspto_base_url + '/' + filename)
    read = BytesIO(resp.read())
    print(f"the download of the file took ", dt.datetime.now() - time1)
    with ZipFile(read) as zip:
        fc = filename.replace('.zip', '.xml')
        with zip.open(fc) as file:
            for line in file:
                stline = line.decode('utf-8')

                if state == WAITING:
                    if f"<{keytag}>" in stline:
                        current_record.append(stline)
                        state = SAVING
                    continue
                if state == SAVING:
                    current_record.append(stline)
                    if f"</{keytag}>" in stline:
                        state = WAITING
                        if current_record:
                            yield ''.join(current_record)
                            current_record = []



def parser(uspto_base_url, filename):
    errors = []
    for record in split_records(uspto_base_url, filename):
        ### skip the XML header
        if '<?xml' in record: continue

        ## try to parse an XML record
        try:
            rr = xmltodict.parse(record)
        except:
            logger.exception(f"Couldn't parse xml")
            continue

        r = rr['case-file']
        transaction_date = r['transaction-date']
        if not (type(transaction_date) is str and len(transaction_date) == 8):
            print(f"unusual date format: {transaction_date}")  # a place for a debugger breakpoint (an unusual case for analysis)
        try:
            if r['case-file-header']['mark-identification'] is None:
                continue
            row = {
                'serial-number': r['serial-number'],
                'mark': r['case-file-header']['mark-identification'],
                'owners': _normalize_owners(r.get('case-file-owners')),
                'status': int(r['case-file-header']['status-code']),
                'transaction-date': transaction_date,
                'statements': _filter_statements(_normalize_statements(r.get('case-file-statements'))),
            }

        except Exception as e:
            if not (e.args[0] == 'mark-identification'):
                logger.exception(f"Couldn't extract data from record: {r}")
            continue

        yield (row)


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


def add_processed_file(processed_files_file, new_processed_file):
    with open(processed_files_file, 'a') as file:
        file.write(new_processed_file + '\n')


def _normalize_statements(statements):
    if statements is None:
        return {}
    st = statements['case-file-statement']
    if type(st) is dict:
        return {st['type-code']: st['text']}
    else:
        return {s['type-code']: s['text'] for s in st}


def _filter_statements(statements):
    def filter_func(tuple_elem):
        return tuple_elem[0].startswith('GS') or tuple_elem[0].startswith('PM')

    return dict(filter(filter_func, statements.items()))


def _normalize_owners(owners):
    if owners is None:
        return []
    owners = owners['case-file-owner']
    if type(owners) is dict:
        return [owners]
    return owners


def parse_date_string(date: str):
    if len(date) == 6:
        return dt.date(
            year=int('20' + date[:2]),
            month=int(date[2:4]),
            day=int(date[4:6])
        )
    elif len(date) == 8:
        return dt.date(
            year=int(date[:4]),
            month=int(date[4:6]),
            day=int(date[6:8])
        )
    else:
        raise NotImplementedError()


def load_sql_template(name, parameters={}, limit=None):
    current_dir = pathlib.Path(__file__).parent.resolve()
    sql_file = current_dir / 'sql' / name
    with open(sql_file) as f:
        sql = f.read()
    for param, value in parameters.items():
        if param.endswith('|datelist'):
            value = ', '.join([f"date '{v.isoformat()}'" for v in value])
        sql = sql.replace('{{' + param + '}}', value)
    if limit:
        sql += f'\nLIMIT {limit}\n'
    return sql


DEFAULT_ABANDON_EXCEPTIONS_FOR_RETRY = {
    AttributeError,
}


def retry_function(retries=3, delay=0, error_log_text='', abandon_exceptions=()):
    """
    Restarts the function a certain number of times when an exception occurs in the function.
    If the function ends without exception, it returns the result. Logs about the exceptions that have occurred
    @param retries: The number of attempts, must be a positive integer
    @param delay: The delay in seconds (float) before the next launch of the function
    @param error_log_text: The text will be written when an exception occurs
    @param abandon_exceptions: Exception types in which retries should not occur more
    @return: the result of function if it ended without exceptions
    """
    assert retries > 0 and type(retries) == int, "the number of attempts must be a positive integer"
    assert delay >= 0, "the delay must be a float positive number in seconds"

    abandon_exceptions = set(abandon_exceptions) | DEFAULT_ABANDON_EXCEPTIONS_FOR_RETRY

    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            thread_info = f"(PID {os. getpid()}, thread {threading.get_ident()})"
            log_prefix = kwargs.get('log_prefix') or kwargs.get('logger_prefix') or kwargs.get('logging_prefix')
            log_prefix = f"{log_prefix} {thread_info}: " if log_prefix else f"{thread_info}: "
            for attempt in range(1, retries + 1):
                try:
                    return f(*args, **kwargs)
                except Exception as e:
                    error_text = f"{log_prefix}{error_log_text} (attempt {attempt}/{retries}):\n{traceback.format_exc()}"
                    if attempt < retries and type(e) not in abandon_exceptions:
                        logger.warning(error_text + f'\nRetrying in {delay} seconds...')
                        if delay > 0:
                            time.sleep(delay)
                    else:
                        logger.error(error_text)
                        raise
        return wrapper
    return decorator


def build_serial_id_mapping(existed_docs: list) -> typing.Dict[str, str]:
    mapping = {}
    for doc in existed_docs:
        id = doc['_additional']['id']
        serial = extract_tm_serial_from_docname(doc['doc_name'])
        mapping[serial] = id
    return mapping


def extract_tm_serial_from_docname(docname: str) -> str:
    return docname.split()[0][2:]
