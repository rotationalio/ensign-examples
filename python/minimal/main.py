import json
import asyncio
from datetime import datetime

from pyensign.ensign import Ensign
from pyensign.events import Event



TOPIC = "chocolate-covered-espresso-beans"

# An asyncio Event object manages an internal flag that can be set to true with the
# set() method and reset to false with the clear() method. The wait() method blocks
# until the flag is set to true. The flag is set to false initially.
# That means we can use it to communicate between the publisher (which should create a
# "wait" signal) and the subscriber (which will acknowledge the event with `set`)
RECEIVED = asyncio.Event()


async def print_single_event(event):
    msg = json.loads(event.data)
    print(
        "At {}, {} sent you the following message: {}".format(
            msg["timestamp"], msg["sender"], msg["message"]
        )
    )
    # Let the program know that the event has been acknowledged by the subscriber
    await RECEIVED.set()


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

    # Set up the subscriber and add a short sleep -- this stuff happens fast!
    # TODO can switch publish/subscribe order after persistence merged in Ensign core
    await client.subscribe(TOPIC, on_event=print_single_event)
    await asyncio.sleep(1)

    # Set up publisher and notify the program to wait until the event is acknowledged
    # by the subscriber
    await client.publish(TOPIC, event)
    await RECEIVED.wait()

if __name__ == "__main__":
    # TODO in Python>3.10
    # TODO need to ignore DeprecationWarning: There is no current event loop
    import warnings
    warnings.filterwarnings("ignore")

    asyncio.get_event_loop().run_until_complete(main())