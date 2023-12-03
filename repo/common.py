from .mongo import store_into_mongo_cloud
from .elasticsearch import store_into_elasticsearch


def store_job_info(job_info):
    # pymongo generates `_id` field when it tries to insert document
    # So when `store_into_mongo_cloud` is called, it will add `_id` field to `job_info`
    # That's why it should call `store_into_elasticsearch` first.
    store_into_elasticsearch(job_info)
    store_into_mongo_cloud(job_info)

    # you can write code like the below
    # store_into_mongo_cloud(job_info)
    # del job_info['_id']
    # store_into_elasticsearch(job_info)
