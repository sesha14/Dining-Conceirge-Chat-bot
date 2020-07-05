import json
import boto3
import datetime
from botocore.vendored import requests
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import csv
from io import BytesIO

def lambda_handler(event, context):
    # TODO implement
    resultData = []

    if event['data_origin'] == 'yelp':
        totalRestaurantCount = 15
        for cuisine in ['japanese','indian','italian','korean','chinese','thai','mexican']:#, 'indian', 'mexican', 'chinese', 'thai', 'french']:japanese

            for i in range(totalRestaurantCount):
                requestData = {
                                "term": cuisine + " restaurants",
                                "location": "manhattan",
                                "limit": 50,
                                "offset": 50*i
                            }
                result = yelpApiCall(requestData)
                resultData = resultData + result

        # Add data to the dynamodDB
        dynamoInsert(resultData)


    #else:

        # get data from s3
    #    resultData = getDataFromS3()

         #Add index data to the ElasticSearch
        #elasticIndexForPrediction(resultData)

    return {
        'statusCode': 200,
        'body': json.dumps('success'),
        'total': 1#len(resultData)
    }

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

def dynamoInsert(restaurants):

    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('yelp-restaurants')


    for each_restaurants in restaurants:

        dataObject = {
            'Id': each_restaurants['id'],
            'alias': each_restaurants['alias'],
            'name': each_restaurants['name'],
            'is_closed': each_restaurants['is_closed'],
            'categories': each_restaurants['categories'],
            'rating': int(each_restaurants['rating']),
            'review_count': each_restaurants['review_count'],
            # 'transactions': each_restaurants['transactions'],
            # 'zip_code': each_restaurants['location']['zip_code'],
            'display_address': each_restaurants['location']['display_address']
        }

        if (each_restaurants['image_url']):
            dataObject['image_url'] = each_restaurants['image_url']

        if (each_restaurants['coordinates'] and each_restaurants['coordinates']['latitude'] and each_restaurants['coordinates']['longitude']):
            dataObject['latitude'] = str(each_restaurants['coordinates']['latitude'])
            dataObject['longitude'] = str(each_restaurants['coordinates']['longitude'])

        if (each_restaurants['phone']):
            dataObject['phone'] = each_restaurants['phone']

        if (each_restaurants['location']['zip_code']):
            dataObject['zip_code'] = each_restaurants['location']['zip_code']


        print ('dataObject', dataObject)
        table.put_item(
               Item={
                   'insertedAtTimestamp': str(datetime.datetime.now()),
                   'info': dataObject,
                   'Id': dataObject['Id']
               }
            )




def getDataFromS3():

    data_key = 'yelpData.csv'
    bucket = 'yelp-data-elastic'
    data_location = 'https://yelp-data-elastic.s3.us-east-2.amazonaws.com/yelpData.csv'

    s3 = boto3.resource(u's3')

    # get a handle on the bucket that holds your file
    bucket = s3.Bucket(u'yelp-data-elastic')

    # get a handle on the object you want (i.e. your file)
    obj = bucket.Object(key=u'yelpData.csv')

    # get the object
    response = obj.get()

    lines = response['Body'].read().splitlines(True)
    final_list = []
    for line in lines:
        final_list.append(line.decode('UTF-8'))

    reader = csv.reader(final_list)

    result = []
    i = 0
    for row in reader:
        if i != 0:
            if float(row[-1]) == 1.0:
                dataObject = {
                    'id': row[1],
                    'cuisine': row[2],
                    'rating': int(row[3]),
                    'review_count': int(row[4]),
                    'score': float(row[5])
                }
                result.append(dataObject)
        i = i+1


    return result
