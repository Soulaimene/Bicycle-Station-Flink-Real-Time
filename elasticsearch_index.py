import json
import requests
from elasticsearch import Elasticsearch

def create_index(client, index, mapping) -> None:
    if not client.indices.exists(index=index):
        headers = {'Content-Type': 'application/json'}
        url = f"http://localhost:9200/bike"
        response = requests.put(url, headers=headers, data=json.dumps(mapping))
        
        if response.status_code == 200:
            print("Index created successfully")
        else:
            print("Error creating index")

if __name__ == "__main__":
    mapping = {
        "mappings": {
            "properties": {
                "numbers": {"type": "integer"},
                "contract_name": {"type": "keyword"},
                "banking": {"type": "text"},
                "bike_stands": {"type": "integer"},
                "available_bike_stands": {"type": "integer"},
                "available_bikes": {"type": "integer"},
                "address": {"type": "text"},
                "status": {"type": "text"},
                "position": {"type": "geo_point"},
                "timestamps": {"type": "text"}
            }
        }
    }

    es = Elasticsearch("http://localhost:9200")
    create_index(client=es, index="bike", mapping=mapping)
    #response = es.indices.delete(index='bike', ignore=[400, 404])

  #