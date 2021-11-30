from elasticsearch import Elasticsearch
import elasticsearch

with Elasticsearch(['http://localhost:9200']) as elastic:
    # elastic.index(
    #     index='index1',
    #     id=0,
    #     document={
    #         'name': 'Matheus',
    #         'surname': 'Bigarelli'
    #     }
    # )

    print(elastic.search(index='index1', query={'match_all': {}}))
