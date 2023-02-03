#%%
from google.cloud import pubsub_v1
publisher = pubsub_v1.PublisherClient()
#%%
isLocal=False
#%%
import certifi
import requests
import json
import pymongo
m_db=pymongo.MongoClient("mongodb+srv://chrislally:lLQg1YFf3Plj@fide-id.yslw4ye.mongodb.net/", tlsCAFile=certifi.where())

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
    
    del entity_final['_id']
    del entity_final['time_updated']
    del entity_final['time_created']
    del entity_final['revision']
    
    return entity_final

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
