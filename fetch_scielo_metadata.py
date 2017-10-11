from io import BytesIO
import argparse
from urllib.parse import urlparse, urlencode, parse_qs, urlunparse
from articlemeta.client import RestfulClient

import requests
from lxml import etree

LIMIT = 100


def parse_xml(xml):
    xml = BytesIO(xml.encode('utf-8'))
    xml_doc = etree.parse(xml)
    return xml_doc


def fetch_documents_metadata_from_query(url):
    """
    Unfortunately the API is for harvesting not for quering. So we need to goes
    through all the records to get those that match is some search criteria.
    To workaround on it, this method retrive SciELO ID's for each document
    resulting from a search query made in the search.scielo.org website.

    In the future we will include some fields for quering directly on the API.

    This method is an generator that will retrieve a "list" of ids to be used as
    identifier to query and retrieve metadata from the SciELO API.

    the id is compounded by the collection identifier and the document identifier
    ex: scl, S0102-311X2016000600601
    """
    rc = RestfulClient()
    uparsed = urlparse(url)
    query = {k: v for k, v in parse_qs(uparsed[4]).items() if 'q' in k or 'filter' in k}
    query['output'] = ['xml']
    query['from'] = ['1']
    query['count'] = [str(LIMIT)]

    while True:
        url = urlunparse(['http', 'search.scielo.org', '/', '', urlencode(query, doseq=True), ''])
        result = requests.get(url, timeout=10)
        query['from'] = [str(int(query['from'][0]) + LIMIT)]

        if not result.status_code == 200:
            continue

        xml = parse_xml(result.text)
        ids = xml.xpath("//result/doc/str[@name='id']/text()")
        print(ids)
        if len(ids) == 0:
            break

        for item in ids:
            col = item[-3:]
            pid = item[:23]
            yield rc.document(pid, col)


def output(documents):
    """
    Make your mess here! All the SciELO metadata will be available for each
    document through the Article Meta API.
    """
    header = [
        'SciELO ID',
        'DOI',
        'original title',
        'ISSN',
        'journal title',
        'total authors',
        'not normalized country',
        'ISO 3166 Affiliation',
        'ISO Language'
    ]

    print(','.join(header))

    for document in documents:

        data = []
        data.append(document.publisher_id)
        data.append(document.doi or '')
        data.append(document.original_title() or '')
        data.append(document.journal.scielo_issn)
        data.append(document.journal.title)
        data.append(str(len(document.authors or [])))
        data.append(';'.join(list(set([i['country'] for i in document.mixed_affiliations if 'country' in i and i['country']]))))
        data.append(';'.join(list(set([i['country_iso_3166'].upper() for i in document.mixed_affiliations if 'country_iso_3166' in i and i['country_iso_3166']]))))
        data.append(';'.join(list(set([i.upper() for i in document.languages() or []]))))

        joined_data = u','.join([u'"%s"' % i.replace(u'"', u'""') for i in data])
        print(joined_data)


def main():
    parser = argparse.ArgumentParser(
        description='Load SciELO IDs from SciELO Search engine'
    )

    parser.add_argument(
        '--search_query_url',
        '-s',
        help='Full URL from the search.scielo.org website containing you filters and query',
        required=True
    )

    args = parser.parse_args()

    pids = fetch_documents_metadata_from_query(args.search_query_url)

    output(pids)


if __name__ == "__main__":

    main()
