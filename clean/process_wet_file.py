'''
Author: Aman
Date: 2023-03-20 16:02:40
Contact: cq335955781@gmail.com
LastEditors: Aman
LastEditTime: 2023-04-22 23:34:55
'''
'''
This is largely borrowed from Facebook cc_net repo.
'''
import logging
from typing import ContextManager, Iterable, Iterator, List, Dict, Optional
from urllib.parse import urlparse
import uuid
from pathlib import Path

logger = logging.getLogger(__name__)

############################################
######## For WET file parsing start ########
############################################
def parse_doc(headers: List[str], doc: List[str]) -> Optional[dict]:
    """Headers format is:
    WARC/1.0
    WARC-Type: conversion
    WARC-Target-URI: [url]
    WARC-Date: [crawldate: 2019-02-15T19:15:59Z]
    WARC-Record-ID: <urn:uuid:8865156e-d5f1-4734-9c68-4b46eaf2bb7e>
    WARC-Refers-To: <urn:uuid:340152e2-65cf-4143-b522-8ce4e2d069d7>
    WARC-Block-Digest: sha1:S3DTWCONT2L6ORTGCY2KXEZ37LNBB7V2
    Content-Type: text/plain
    Content-Length: 7743
    """
    if not headers or not doc:
        return None

    try:
        warc_type = headers[1].split()[1]
        if warc_type != "conversion":
            return None
        url = headers[2].split()[1]
        date = headers[3].split()[1]
        digest = headers[6].split()[1]
        try:
            length = int(headers[-2].split()[1])
        except:
            length = 0
    except Exception as e:
        logger.warning("Can't parse header:", e, headers)
        return None

    # Docs are separated by two empty lines.
    last = None
    if not doc[-1] and not doc[-2]:
        last = -2
    title, doc = doc[0], doc[1:last]

    return {
        "url": url,
        "date_download": date,
        "digest": digest,
        "length": length,
        "nlines": len(doc),
        "source_domain": urlparse(url).netloc,
        "title": title,
        "raw_content": "\n".join(doc),
    }


def group_by_docs(warc_lines: Iterable[str]) -> Iterable[dict]:
    doc: List[str] = []
    headers, read_headers = [], True
    for warc in warc_lines:
        warc = warc.strip()
        if read_headers:
            headers.append(warc)
            read_headers = warc != ""
            continue

        if warc == "WARC/1.0":
            # We reached the beginning of the new doc.
            parsed = parse_doc(headers, doc)
            if parsed is not None:
                yield parsed
            headers, doc, read_headers = [warc], [], True
            continue

        doc.append(warc)

    # Return the last document
    if doc:
        parsed = parse_doc(headers, doc)
        if parsed is not None:
            yield parsed


def parse_warc_file(lines: Iterable[str], min_len: int = 1) -> Iterator[dict]:
    n_doc = 0
    n_ok = 0
    for doc in group_by_docs(lines):
        n_doc += 1
        if not doc or len(doc["raw_content"]) < min_len:
            continue
        n_ok += 1
        yield doc
    if n_doc > 0:
        logger.info(f"Kept {n_ok:_d} documents over {n_doc:_d} ({n_ok / n_doc:.1%}).")
    else:
        logger.info(f"Found no documents")

############################################
######### For WET file parsing end #########
############################################


def write_warc_file(headers: List[str], documents: List[Dict[str, str]], save_path: str) -> None:
    '''
    This is for restoring the fore-cleaned data back to wet files, written by ChatGPT.
    '''
    with open(save_path, 'w') as f:
        # write .warc.wet header
        for header_line in headers:
            f.write(f'{header_line}\r\n')
        for doc in documents:
            # construct .warc.wet header
            warc_record_id = f'<urn:uuid:{uuid.uuid4()}>'
            warc_refers_to = f'<urn:uuid:{uuid.uuid4()}>'
            warc_headers = '\r\n'.join([
                'WARC/1.0',
                'WARC-Type: conversion',
                f'WARC-Target-URI: {doc["url"]}',
                f'WARC-Date: {doc["date_download"]}',
                f'WARC-Record-ID: {warc_record_id}',
                f'WARC-Refers-To: {warc_refers_to}',
                f'WARC-Block-Digest: sha1:{doc["digest"]}',
                'Content-Type: text/plain',
                f'Content-Length: {doc["length"]}',
            ])
            f.write(f'{warc_headers}\r\n\r\n')

            # write to file
            f.write(f'{doc["title"].rstrip()}\r\n')
            f.write(f'{doc["raw_content"].rstrip()}\r\n')

            # end of content, leave 2 empty lines between adjacent documents
            f.write('\r\n\n')



