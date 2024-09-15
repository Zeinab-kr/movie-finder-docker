from fastapi import FastAPI
from elasticsearch import Elasticsearch
import redis
import json
import requests

app = FastAPI()

# Initialize Elasticsearch client with authentication and certificate verification disabled
es = Elasticsearch(
    [{'host': 'elasticsearch', 'port': 9200, 'scheme': 'http'}],
    basic_auth=('elastic', 'Zeinab801224'),
    verify_certs=False
)

# Initialize Redis client
redis_client = redis.StrictRedis(host='redis', port=6379, db=0)


@app.get("/")
async def home():
    return {"message": "Works!"}


# Search endpoint
@app.get("/search/")
async def search_movie(query: str):
    cached_result = redis_client.get(query)

    if cached_result:
        print("searched in redis")
        return cached_result

    # Search in Elasticsearch
    es_result = es.search(index='your_index_name', body={'query': {'match': {'Series_Title': query}}})

    if es_result['hits']['total']['value'] > 0:
        # Extract the relevant data from the Elasticsearch response
        movies = [hit['_source'] for hit in es_result['hits']['hits']]

        # Cache the result in Redis
        redis_client.set(query, json.dumps(movies))
        print("searched in elasticsearch")
        return movies
    else:
        url = "https://imdb146.p.rapidapi.com/v1/find/"
        querystring = {"query": query}
        headers = {
            "X-RapidAPI-Key": "9180af86efmshf3c5b0a594242b7p12ce87jsn6333b750b986",
            "X-RapidAPI-Host": "imdb146.p.rapidapi.com"
        }
        try:
            response = requests.get(url, headers=headers, params=querystring)
        except:
            response = "Could not find the movie"
            print("An error occurred")

        redis_client.set(query, json.dumps(response.json()))
        print("searched in imdb api")
        return response.json()

