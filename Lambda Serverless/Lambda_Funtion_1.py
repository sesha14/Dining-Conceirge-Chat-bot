import json
import boto3
import datetime
import dateutil.parser
import os
import time


def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }
    
def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }

    return response
    
def delegate(session_attributes, slots):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Delegate',
            'slots': slots
        }
    }

def is_invalid_date(date):
    try:
        parsed_date = dateutil.parser.parse(date).date()
        return parsed_date < datetime.date.today()
    except ValueError:
        return False

def is_invalid_time(time, date):
    try:
        parsed_time = dateutil.parser.parse(time).timestamp()
        parsed_date = dateutil.parser.parse(date).date()
        return parsed_date == datetime.date.today() and parsed_time < datetime.datetime.now().timestamp()
    except ValueError:
        return False;
        
def validate_restaurant_intent(intent_request):
    session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
    
    return close(
        session_attributes,
        'Fulfilled',
        {
            'contentType': 'PlainText',
            'content': 'I\'m sorry, I can only give you restaurant suggestions.'
        }
    )

def greet_user(intent_request):
    session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
    
    return close(
        session_attributes,
        'Fulfilled',
        {
            'contentType': 'PlainText',
            'content': 'Hello there, how can I help you?'
        }
    )
    
def respond_to_thanks(intent_request):
    session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
    
    return close(
        session_attributes,
        'Fulfilled',
        {
            'contentType': 'PlainText',
            'content': 'You\'re welcome!'
        }
    )

def suggest_restaurant(intent_request):
    slots = intent_request['currentIntent']['slots']
    location = slots.get('Location')
    cuisine = slots.get('Cuisine')
    party = slots.get('Party')
    date = slots.get('Date')
    time = slots.get('Time')
    phone = slots.get('Phone')
    
    session_attributes = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
    
    party_int = int(party) if party is not None else None
        
    if intent_request['invocationSource'] == 'DialogCodeHook':
        invalidSlot = None
        invalidMessage = None
        
        cuisines = ["chinese", "mexican", "korean", "thai", "japanese", 
        "indian","italian"]
        
        if party_int is not None and (party_int < 1 or party_int > 50):
            invalidSlot = 'Party'
            invalidMessage ='Party size can be from one to fifty people. How many people are in your party?'
        elif location and location.lower() != 'manhattan':
            invalidSlot = 'Location'
            invalidMessage = 'I\'m sorry, that location is not supported. Please try another city or area.'
        elif cuisine and cuisine.lower() not in cuisines:
            invalidSlot = 'Cuisine'
            invalidMessage = 'I\'m sorry we don\'t support that cuisine. Please choose another cuisine?'
        elif date and is_invalid_date(date):
            invalidSlot = 'Date'
            invalidMessage = 'The date can be any date from today onwards. What date would you like to dine on?'
        elif time and is_invalid_time(time, date):
            invalidSlot = 'Time'
            invalidMessage = 'The time can be any time after now. What time would you like to dine?'
        elif phone is not None and (len(phone) < 10 or len(phone) > 12 or (len(phone) == 11 and phone[0] != '1') or (len(phone) == 12 and (phone[0] != '+' or phone[1] != '1'))):
            invalidSlot = 'Phone'
            invalidMessage = 'The phone number must be a valid US phone number. What phone number would you like to put down?'
            
        if invalidSlot:
            slots[invalidSlot] = None
            return elicit_slot(
                session_attributes,
                intent_request['currentIntent']['name'],
                slots,
                invalidSlot,
                {'contentType': 'PlainText', 'content': invalidMessage}
            )
        
        return delegate(session_attributes, slots)
        
    phone = ('+1' if len(phone) == 10 else '+' if len(phone) == 11 else '') + phone
    
    preferences = json.dumps({
        'Location': location,
        'Cuisine': cuisine,
        'Party': party,
        'Date': date,
        'Time': time,
        'Phone': phone
    })
    
    client = boto3.client('sqs')
    client.send_message(
        QueueUrl='SQS-QUEUE-URL', 
        MessageBody=preferences,
        MessageAttributes={
            'MessageType': {
                'StringValue': 'GetSuggestionForDining',
                'DataType': 'String'
            }
        }
    )

    return close(
        session_attributes,
        'Fulfilled',
        {
            'contentType': 'PlainText',
            'content': 'You\'re all set. Expect my suggestions shortly at {}! Have a good day.'.format(phone)
        }
    )
    
def dispatch(intent_request):

    intent_name = intent_request['currentIntent']['name']

    if intent_name == 'ValidateRestaurantIntent':
        return validate_restaurant_intent(intent_request)
    if intent_name == 'GreetingIntent':
        return greet_user(intent_request)
    elif intent_name == 'DiningSuggestionsIntent':
        return suggest_restaurant(intent_request)
    elif intent_name == 'ThankYouIntent':
        return respond_to_thanks(intent_request)

    raise Exception('Intent with name ' + intent_name + ' not supported')


# --- Main handler ---


def lambda_handler(event, context):
    os.environ["TZ"] = 'America/New_York'
    time.tzset()

    return dispatch(event)
