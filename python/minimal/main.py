import json
import asyncio
from datetime import datetime

from pyensign.ensign import Ensign
from pyensign.events import Event

TOPIC = "chocolate-covered-espresso-beans"

async def publish(client, topic, events):
    await asyncio.sleep(1)
    errors = await client.publish(topic, events)
    if errors:
        print("Failed to publish events: {}".format(errors))
    return errors

async def subscribe(client, topic):
    id = await(client.topic_id(topic))
    async for event in client.subscribe(id):
        msg = json.loads(event.data)
        print("At {}, {} sent you the following message: {}".format(msg["timestamp"], msg["sender"], msg["message"]))
        return event

async def main():
    # Create an Ensign client
    client = Ensign(
        # endpoint="staging.ensign.world:443", # uncomment if in staging
        # auth_url="https://auth.ensign.world" # uncomment if in staging
    )

    # Create topic if it doesn't exist
    if not await client.topic_exists(TOPIC):
        await client.create_topic(TOPIC)

    # Create an event with some data
    msg = {
        "sender": "Enson the Sea Otter",
        "timestamp": datetime.now().isoformat(),
        "message": "You're looking smart today!",
    }
    data = json.dumps(msg).encode("utf-8")
    event = Event(data, mimetype="application/json")

    # Create the publisher and subscriber tasks
    pub = publish(client, TOPIC, event)
    sub = subscribe(client, TOPIC)

    # Wait for the tasks to complete
    await asyncio.gather(pub, sub)

if __name__ == "__main__":
    asyncio.run(main())