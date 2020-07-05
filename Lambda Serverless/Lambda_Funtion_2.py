import json
import boto3
import ast
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError

def lambda_handler(event, context):
    #sendTextToUser()
    print ("inside lambda handler")
    sqs = boto3.client('sqs')
    queue_url = 'SQS-QUEUE-URL'

    # Receive message from SQS queue
    response = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=[
            'SentTimestamp'
        ],
        MaxNumberOfMessages=1,
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout=0,
        WaitTimeSeconds=15
    )

    if (response and 'Messages' in response):

        # restaurant original host

        
        host = 'HOST-URL'

        region = 'us-east-1'
        service = 'es'
        credentials = boto3.Session().get_credentials()
        awsauth = AWS4Auth('ACCESS-KEY','SECRET-ACCESS-KEY' , region, service)

        es = Elasticsearch(
            hosts = [{'host': host, 'port': 443}],
            http_auth = awsauth,
            use_ssl = True,
            verify_certs = True,
            connection_class = RequestsHttpConnection
        )

        dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
        table = dynamodb.Table('yelp-restaurants')

        for each_message in response['Messages']:

            message = each_message
            receipt_handle = message['ReceiptHandle']
            req_attributes = message['MessageAttributes']
            messageBody = message['Body']
            messageBody = ast.literal_eval(messageBody)
            res_category = messageBody['Cuisine']
            print (res_category)
            searchData = es.search(index="restaurants", body={
                                                             "query": {
                                                                 "match": {
                                                                    "categories.title": res_category
                                                                 }}})


            #print("Got %d Hits:" % searchData['hits']['hits'])

            businessIds = []
            for hit in searchData['hits']['hits']:
                #print (hit)
                businessIds.append(hit['_source']['Id'])

            # Call the dynemoDB
            resultData = getDynemoDbData(table, messageBody, businessIds)
            print (resultData)
            #print ('req_attributes----', req_attributes)

            # send the email
            sendMailToUser(req_attributes, resultData)
            sendTextToUser(messageBody,resultData)
            # Delete received message from queue
            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
            )


    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

def getDynemoDbData(table, requestData, businessIds):
    #print (requestData)
    
  
    if len(businessIds) <= 0:
        return 'We can not find any restaurant under this description, please try again.'

    textString = "Hello! Here are my " + requestData['Cuisine'] + " restaurant suggestions for " + requestData['Party'] +" people, for " + requestData['Date'] + " at " + requestData['Time'] + ". "
    count = 1

    for business in businessIds:
        responseData = table.query(KeyConditionExpression=Key('Id').eq(business))

        if (responseData and len(responseData['Items']) >= 1 and responseData['Items'][0]['info']):
            responseData = responseData['Items'][0]['info']
            display_address = ', '.join(responseData['display_address'])

            textString = textString + " " + str(count) + "." + responseData['name'] + ", located at " + display_address + " "
            count += 1

    textString = textString + " Hope you like the restaurant!"
    #print (textString)
    return textString
def sendTextToUser(requestData,resultData):

    credentials = boto3.Session().get_credentials()
    RECIPIENT = requestData['Phone']

    print("RECIPIENT", RECIPIENT)
    print("resultData", resultData)

     # Create an SNS client
    sns = boto3.client(
         "sns",
         aws_access_key_id= "YOUR-ACCESS-KEY",
         aws_secret_access_key="YOUR-SECRET-ACCESS-KEY",
         region_name="us-east-1"
    )

     # Send your sms message.
    try:
        response = sns.publish(
            PhoneNumber= RECIPIENT,
            Message= resultData
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("text message sent")
        print(response['MessageId'])
def sendMailToUser(requestData, resultData):

    SENDER = "YOUR-EMAIL"
    RECIPIENT = "YOUR-EMAIL"
    AWS_REGION = "us-east-1"

    SUBJECT = "Your Dining recommendations"

    BODY_TEXT = ("AWS project in (Python)")

    # The HTML body of the email.
    BODY_HTML = """<html>
    <head></head>
    <body>
      <h1>Restaurant Suggestions</h1>
      <p>Hi User, Following are your restaurant recommendations</p>
      <p>""" + resultData + """</p>
    </body>
    </html>
                """

    # The character encoding for the email.
    CHARSET = "UTF-8"

    # Create a new SES resource and specify a region.
    client = boto3.client('ses',region_name=AWS_REGION)

    # return true
    # Try to send the email.
    try:
        #Provide the contents of the email.
        response = client.send_email(
            Destination={
                'ToAddresses': [
                    RECIPIENT,
                ],
            },
            Message={
                'Body': {
                    'Html': {
                        'Charset': CHARSET,
                        'Data': BODY_HTML,
                    },
                    'Text': {
                        'Charset': CHARSET,
                        'Data': BODY_TEXT,
                    },
                },
                'Subject': {
                    # 'Charset': CHARSET,
                    'Data': SUBJECT,
                },
            },
            Source=SENDER,
            # # If you are not using a configuration set, comment or delete the
            # # following line
            # ConfigurationSetName=CONFIGURATION_SET,
        )
    # Display an error if something goes wrong.
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])

