from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
import pandas as pd

elasticsearch = Elasticsearch(host="localhost", port=9200)


# Question 1
def basic_search(field, value):
    try:
        res = elasticsearch.search(index="bx-books", query={
            "match": {
              f"{field}": f"{value}"
            }},
            sort=[
                {
                    "_score": {
                        "order": "desc"
                    }
                }
            ]
        )
    except value.error as error:
        print("Error occurred." + error)
        return

    if (res["hits"]["total"]["value"] == 0):
        print("No data has been returned!")
        return

    for hit in res["hits"]["hits"]:
        print("Elastics score:", hit["_score"], "--- Isbn:", hit["_source"]["isbn"], "---", f"{field}:",  hit["_source"][f"{field}"])


if __name__ == "__main__":

    while True:
        field = input("Give me a field: ")
        value = input("Give me a value: ")
        basic_search(field, value)
