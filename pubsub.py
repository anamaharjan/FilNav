import json

from google.cloud import pubsub_v1

publisher = pubsub_v1.PublisherClient()

def publish(request):
    request_json = request.get_json(silent=True)
    topic_name = request_json.get("topic")
    message = request_json.get("message")
    if not topic_name or not message:
        return ('Missing "topic" and/or "message" parameter.', 400)
    print(f'Publishing message to topic {topic_name}')
    topic_path = publisher.topic_path("$GOOGLE_PROJECT_ID", topic_name)

    message_json = json.dumps({"data":message} )
    message_bytes = message_json.encode('utf-8')

    try:
        publish_future = publisher.publish(topic_path, data=message_bytes)
        publish_future.result()  # Verify the publish succeeded
        return 'Message published.'
    except Exception as e:
        print(e)
        return (e, 500)
