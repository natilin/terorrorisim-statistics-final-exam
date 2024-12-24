from datetime import datetime

from app.db.elastic_database import get_elasticsearch_client


def get_terror_events_id_by_keyword(keyword: str, limit: int=100):
    es = get_elasticsearch_client()
    query = {
        "query": {
            "match": {
                "summary": keyword
            }
        },
        "size": limit
    }

    response = es.search(index= "terror_event", body=query)['hits']['hits']
    return [res["_id"] for res in response]



def get_terror_events_id_by_keyword_and_date(keyword: str, start_date: datetime, end_date: datetime, limit: int=100):
    es = get_elasticsearch_client()
    query = {
        "query": {
            "bool": {
                "must": {
                    "match": {
                        "summary": keyword
                    }
                },
                "filter": [
                    {
                        "range": {
                            "date": {
                                "gte": start_date,
                                "lte": end_date
                            }
                        }
                    }
                ]
            }
        },
        "size": limit
    }
    response = es.search(index= "terror_event", body=query)['hits']['hits']
    return [res["_id"] for res in response]


