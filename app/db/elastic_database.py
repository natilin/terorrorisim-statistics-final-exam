from elasticsearch import Elasticsearch



def get_elasticsearch_client():
    client = Elasticsearch(
        ['http://localhost:9200'],
        basic_auth=("elastic", "3uDiv6AS"),
        verify_certs=False
    )
    return client