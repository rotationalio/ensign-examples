import json
import asyncio
from datetime import datetime

from pyensign.ensign import Ensign
from pyensign.events import Event
from pyensign.api.v1beta1.ensign_pb2 import Nack   



TOPIC = "documents"

# An asyncio Event object manages an internal flag that can be set to true with the
# set() method and reset to false with the clear() method. The wait() method blocks
# until the flag is set to true. The flag is set to false initially.
# That means we can use it to communicate between the publisher (which should create a
# "wait" signal) and the subscriber (which will acknowledge the event with `set`)
RECEIVED = asyncio.Event()


async def print_single_event(event):
    
    msg = json.loads(event.data)
    
    print(msg)

 

async def handle_event(event):
    """
    Decode and ack the event.
    """
    try:
        data = json.loads(event.data)
    except json.JSONDecodeError:
        print("Received invalid JSON in event payload:", event.data)
        await event.nack(Nack.Code.UNKNOWN_TYPE)
        return

    print("New document received:", data)
    await event.ack()

async def main():
    # Create an Ensign client
    client = Ensign()
    
    #await client.ensure_topic_exists(TOPIC)
    topic_id = await client.topic_id(TOPIC)
    
    # Set up the subscriber and add a short sleep -- this stuff happens fast!
    # TODO can switch publish/subscribe order after persistence merged in Ensign core
    await client.subscribe(topic_id, on_event=handle_event)
    await asyncio.Future()
    
    #await client.subscribe(TOPIC, on_event=print_single_event)
    #await asyncio.sleep(5)

if __name__ == "__main__":
    # TODO in Python>3.10
    # TODO need to ignore DeprecationWarning: There is no current event loop
    import warnings
    warnings.filterwarnings("ignore")
    #asyncio.run(main())
    asyncio.get_event_loop().run_until_complete(main())