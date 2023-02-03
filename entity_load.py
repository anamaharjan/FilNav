#%%
from google.cloud import pubsub_v1
publisher = pubsub_v1.PublisherClient()
#%%
isLocal=False
#%%
import certifi
import requests
import json
from urllib import parse
import pymongo
import time
import copy
from bson.objectid import ObjectId
import base64
m_db=pymongo.MongoClient("mongodb+srv://$USER:$PASS@$DB.$KEY.mongodb.net/", tlsCAFile=certifi.where())

def pubsub(topic_name,message):
    if isLocal:
        return LOCALpubsub(topic_name,message)
    
    topic_path = publisher.topic_path("total-scion-368611", topic_name)
    message_json = json.dumps({"data":message} )
    message_bytes = message_json.encode('utf-8')
    try:
        publish_future = publisher.publish(topic_path, data=message_bytes)
        publish_future.result()  # Verify the publish succeeded
        return 'Message published.'
    except Exception as e:
        print(e)
        return (e, 500)

def LOCALpubsub(topic_name,message):
    url = "https://us-central1-total-scion-368611.cloudfunctions.net/pubsub"
    payload = json.dumps({
    "topic": topic_name,
    "message": message
    })
    headers = {
    'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    print(response.text)

def fullEntity(fide_id):
    entity_final=m_db["entity_db"]["entities"].find_one({"fide_id":fide_id})
    
    
    if entity_final.get('times'):
        entity_time=entity_final['times']
        if entity_time.get('start'):
            entity_time['start_str']=time.strftime('%m-%d-%Y', time.localtime(entity_time['start']))
        
        entity_final['times']=entity_time
    
    if entity_final.get('team_size'):
        team_size=entity_final['team_size']
        if team_size.get('count') and team_size.get('range') and team_size.get('range')!=0:
            s_low=team_size.get('count')-team_size.get('range')
            s_high=team_size.get('count')+team_size.get('range')
            team_size['range_str']=str(s_low)+"-"+str(s_high)+" people"
        elif team_size.get('count'):
            team_size['range_str']=str(team_size.get('count'))+" people"
        entity_final['team_size']=team_size
    
    del entity_final['_id']
    del entity_final['time_updated']
    del entity_final['time_created']
    del entity_final['revision']
    
    return entity_final

#%%
# fide_id="f3365482381"
# fullEntity(fide_id)
#%%

def entityLoad(request):
    request_json = request.get_json(silent=True)
    topic_name = request_json.get("topic")
    message = request_json.get("message")
    if not topic_name or not message:
        return ('Missing "topic" and/or "message" parameter.', 400)
    print(f'Getting ent message to topic {topic_name}')

    
    try:
        if topic_name=="full_entity":
            fide_id=message['fide_id']
            return_me=fullEntity(fide_id)
        else:
            return_me="topic unknown: "+topic_name
        return return_me
    except Exception as e:
        print(e)
        return (e, 500)
    
# %%
