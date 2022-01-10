
from elasticsearch import Elasticsearch

elasticsearch = Elasticsearch(host="localhost", port=9200)


# Question 2
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
    except:
        print("There has been an error in retrieving the data!")
        return

    if (res["hits"]["total"]["value"] == 0):
        print("No data has been returned!")
        return

    for hit in res["hits"]["hits"]:
        print(hit["_score"])


def user_rating_search(uid, isbn):
    try:
        res = elasticsearch.search(index="bx-book-ratings-reindex", query={
            "bool": {
              "must": [
                {"match": {
                  "isbn": f"{isbn}"
                  }
                },
                {
                  "match": {
                    "uid": f"{uid}"
                  }
                }

              ]
            }
          })
    except:
        print("There has been an error in retrieving the data!")

    if (res["hits"]["total"]["value"] == 0):
        print("No data has been returned!")
        return
    else:
        print(res["hits"]["total"]["value"], " document has been received!")

    for hit in res["hits"]["hits"]:
        print("Document info -->", hit["_source"])

    hit_rating = res["hits"]["hits"][0]["_source"]["rating"]
    return int(hit_rating)


def all_user_ratings_search(uid, isbn):

    try:
        res = elasticsearch.search(index="bx-book-ratings-reindex", query={
            "bool": {
              "must": [
                {"match": {"isbn": f"{isbn}"}}
              ],
              "must_not": [
                {"match": {
                  "uid": f"{uid}"
                }}
              ]
            }
          },
          aggs={
            "avg_rating": {
              "avg": {
                "field": "rating"
              }
            }
          })
    except:
        print("There has been an error in retrieving the data!")

    if (res["hits"]["total"]["value"] == 0):
        print("No data has been returned!")
        return
    else:
        print(res["hits"]["total"]["value"], "document/s has/have been received!")

    # for hit in res["hits"]["hits"]:
    #     print("Document info -->", hit["_source"])

    return res["aggregations"]["avg_rating"]["value"]


if __name__ == "__main__":


    user_rating = user_rating_search("168047", "0671789422")
    print("User rating --> ", user_rating)
    print("\n")
    avg_rating = all_user_ratings_search("168047", "0671789422")
    print("Other users average rating --> ", avg_rating)
