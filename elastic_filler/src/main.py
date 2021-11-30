"""O propósito deste módulo é iniciar o elastic search com dados dummy."""
import os
import time

from elasticsearch import Elasticsearch


def fill_match(criteria: dict):
    docs = None

    http_auth = (os.getenv('REMOTE_ELASTIC_USER'), os.getenv('REMOTE_ELASTIC_PASSWORD'))

    with Elasticsearch([os.getenv('REMOTE_ELASTIC_URL')], http_auth=http_auth) as elastic:
        docs = elastic.search(
            index=os.getenv('EB_INDEX'),
            size=10000,
            query={
                'match_phrase': {
                    list(criteria.keys())[0]: list(criteria.values())[0]
                }
            }
        )
        if docs and 'hits' in docs:
            docs = docs['hits']['hits']

    # print(docs)

    if docs is not None:
        with Elasticsearch([os.getenv('LOCAL_ELASTIC_URL')]) as elastic:
            for i, doc in enumerate(docs):
                elastic.index(
                    index=os.getenv('EB_INDEX'),
                    id=doc['_id'],
                    document=doc['_source']
                )



def main():
    fill_match({
        'segmento': 'serviços'
    })

    fill_match({
        'segmento': 'comércio'
    })

    fill_match({
        'segmento': 'indústria'
    })

    fill_match({
        'segmento': 'construção civil'
    })

    fill_match({
        'segmento': 'agropecuária'
    })

if __name__ == '__main__':
    main()
