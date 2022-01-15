import nltk
from elasticsearch import Elasticsearch
import re
from nltk import word_tokenize
from gensim.models import Word2Vec
from sklearn.decomposition import PCA
from matplotlib import pyplot
elasticsearch = Elasticsearch(host="localhost", port=9200)


# Question 3
def basic_search(field, value):
    try:
        res = elasticsearch.search(index="bx-books", from_=0, size=10000, query={
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

    return res


# Finds the rating that a specific uid has given to a book of specific isbn returning it as an integer, if it doesnt find any rating then it returns None
def user_rating_search(uid, isbn):
    try:
        res = elasticsearch.search(index="bx-book-ratings", query={
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
        return
    else:
        hit_rating = res["hits"]["hits"][0]["_source"]["rating"]
        return float(hit_rating)


#Calculates the average rating given to a book, note that we don't use the specified users rating in this calculation
def calculate_all_user_ratings(uid, isbn):
    total_rating = 0
    counter = 0
    try:
        res = elasticsearch.search(index="bx-book-ratings", from_=0, size=10000, query={
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
        })
    except:
        print("There has been an error in retrieving the data!")
        return

    if res["hits"]["total"]["value"] == 0:
        print("There are no ratings for this book")
        return

    for hit in res["hits"]["hits"]:
        counter += 1
        total_rating += float(hit["_source"]["rating"])

    average_rating = total_rating / counter
    return float(average_rating)


if __name__ == "__main__":
    result = basic_search("book_title", "Classical Mythology")
    sentence = result["hits"]["hits"][0]["_source"]["summary"]

    sentence = re.split('\s+', sentence)

    print(sentence, "\n")
    new_sentence = [word_tokenize(word) for word in sentence]
    print(new_sentence, "\n")

    #Making the model for this sentence
    model = Word2Vec(new_sentence, min_count=1)
    words = list(model.wv.index_to_key)
    print(words, "\n")


    #Word vectors
    word_vectors = model.wv.vectors
    # print(word_vectors)

    #Visualising
    X = model.wv[model.wv.index_to_key]
    pca = PCA(n_components=2)
    result = pca.fit_transform(X)
    pyplot.scatter(result[:, 0], result[:, 1])

    for i, word in enumerate(words):
        pyplot.annotate(word, xy=(result[i, 0], result[i, 1]))
    pyplot.show()








