import datetime as dt
import logging
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
    return dt.date(
        year=int(date[:4]),
        month=int(date[4:6]),
        day=int(date[6:8])
    )
