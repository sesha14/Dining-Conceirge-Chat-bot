import json
import requests
import boto3
from requests_aws4auth import AWS4Auth

from elasticsearch import Elasticsearch, RequestsHttpConnection
    #from requests_aws4auth import AWS4Auth




def yelpApiCall(requestData):

    url = "https://api.yelp.com/v3/businesses/search"

    querystring = requestData

    payload = ""
    headers = {
        'Authorization': "Bearer YOUR API_KEY",
        'cache-control': "no-cache",
        'Postman-Token': "postman_token"
        }

    response = requests.request("GET", url, data=payload, headers=headers, params=querystring)
    message = json.loads(response.text)
    print (message)

    if len(message['businesses']) <= 0:
        return []

    return message['businesses']



resultData = []
totalRestaurantCount = 15
for cuisine in ['italian','indian','chinese','thai','korean','japanese','mexican']:
    #, 'indian', 'mexican', 'chinese', 'thai', 'french']:japanese

    for i in range(totalRestaurantCount):
        requestData = {
        "term": cuisine + " restaurants",
        "location": "manhattan",
        "limit": 50,
        "offset": 50*i
        }
        result = yelpApiCall(requestData)
        resultData = resultData + result





    #host = 'search-restaurant-aojp7dfepez3ra4uswwnbx5tbu.us-east-2.es.amazonaws.com' # For example, my-test-domain.us-east-1.es.amazonaws.com

host = 'search-restaurant-g56zesqy2cp7vled5m5ivgpep4.us-east-1.es.amazonaws.com'
region = 'us-east-1'

service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service)

es = Elasticsearch(
    hosts = [{'host': host, 'port': 443}],
    http_auth = awsauth,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection
    )

for each_restaurants in resultData:

    dataObject = {
        'Id': each_restaurants['id'],
        'alias': each_restaurants['alias'],
        'name': each_restaurants['name'],
        'categories': each_restaurants['categories']
        }

            # alreadyExists = es.indices.exists(index="restaurants")

    print ('dataObject', dataObject)

            # if alreadyExists:
    es.index(index="restaurants", doc_type="Restaurant", id=each_restaurants['id'], body=dataObject, refresh=True)
            # else:
            #     es.create(index="restaurants", doc_type="Restaurant", body=dataObject)
