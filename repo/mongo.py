import os
from typing import List, Dict

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError


def get_cloud_url():
    return os.environ.get('X-SongUser-MongoCloud-Url')


def get_mongo_client():
    url = get_cloud_url()
    client = MongoClient(url)
    return client


def close_mongo_client(client: MongoClient):
    client.close()


def insert_document(client: MongoClient, db_name: str, coll_name: str, doc: dict):
    client[db_name][coll_name].insert_one(doc)


def insert_documents(client: MongoClient, db_name: str, coll_name: str, docs: List[Dict]):
    client[db_name][coll_name].insert_many(docs)


def store_into_mongo_cloud(job_info):
    db_name = 'job'
    coll_name = 'job_info'
    client = get_mongo_client()
    try:
        insert_document(client, db_name, coll_name, job_info)
    except DuplicateKeyError:
        print(f"There is already duplicated job info with same link({job_info['link']})")
    finally:
        close_mongo_client(client)
