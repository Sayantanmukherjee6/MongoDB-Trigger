import pymongo,click
import logging
from bson import json_util


def run(database, collection, uri):
    client = pymongo.MongoClient(uri)
    db = client.get_database(database)
    col = db.get_collection(collection)

    cursor = col.watch(full_document="updateLookup")

    with cursor as stream:
        for change in stream:
            click.echo(json_util.dumps(change))


if __name__ == "__main__":
	run("test_airflow","pdf_test","mongodb://localhost:27017")