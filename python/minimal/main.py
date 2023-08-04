import os
import json
import asyncio
from datetime import datetime

from pyensign.ensign import Ensign
from pyensign.events import Event

TOPIC = "chocolate-covered-espresso-beans"

async def main():
    # Create an Ensign client
    client = Ensign(
        # endpoint="staging.ensign.world:443", # uncomment if in staging
        # auth_url="https://auth.ensign.world" # uncomment if in staging
    )

    # Create topic if it doesn't exist
    await client.ensure_topic_exists(TOPIC)

    # Create an event with some data
    msg = {
        "sender": "Enson the Sea Otter",
        "timestamp": datetime.now().isoformat(),
        "message": "You're looking smart today!",
    }
    data = json.dumps(msg).encode("utf-8")
    event = Event(data, mimetype="application/json")

    # Set up the publisher to publish the event
    async def publish():
        await client.publish(TOPIC, event)

    # Set up the subscriber to receive the event
    async def subscribe():
        async for event in client.subscribe(TOPIC):
            msg = json.loads(event.data)
            print(
            "At {}, {} sent you the following message: {}".format(
                msg["timestamp"], msg["sender"], msg["message"]
            ))

            # Acknowledge the event back to Ensign
            await event.ack()
            return

    # Run the publisher and subscriber concurrently
    await asyncio.gather(publish(), subscribe())

    # Hack to avoid a bug where the process hangs on exit
    os._exit(0)

if __name__ == "__main__":
    asyncio.run(main())