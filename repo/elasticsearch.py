from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError, NotFoundError


def get_instance() -> Elasticsearch:
    request_timeout = 1.0

    try:
        es_instance = Elasticsearch('http://localhost:9200', request_timeout=request_timeout)
        if not es_instance.options(request_timeout=request_timeout).ping():
            raise ValueError("Connection failed")
        return es_instance
    except ConnectionError:
        print("Failed to connect to Elasticsearch")
    except NotFoundError:
        print("Elasticsearch server not found")
    except Exception as e:
        print(f"An error occurred: {e}")


def get_index_name():
    return 'job_info'


def store_into_elasticsearch(job_info):
    es = get_instance()
    index = get_index_name()
    res = es.index(index=index, document=job_info)
    print("Document indexed:", res['result'])
