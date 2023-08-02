import os
import json
import asyncio
from datetime import datetime

import requests
from pyensign.events import Event
from pyensign.ensign import Ensign

class WeatherPublisher:
    def __init__(self, topic="current-weather", location="Washington, DC"):
        """
        Create a publisher that publishes weather events for a location to a topic.
        """
        self.topic = topic
        self.location = location
        self.weather_api_key = os.environ.get("WEATHER_API_KEY")
        self.ensign = Ensign()

    async def print_ack(self, ack):
        ts = datetime.fromtimestamp(ack.committed.seconds + ack.committed.nanos / 1e9)
        print("Event committed at {}".format(ts))

    async def print_nack(self, nack):
        print("Event was not committed with error {}: {}".format(nack.code, nack.error))

    async def recv_and_publish(self):
        """
        Receive weather events and publish them to the topic.
        """

        # Ensure the topic exists
        await self.ensign.ensure_topic_exists(self.topic)

        while True:
            # Make a request to the weather API
            response = requests.get("http://api.weatherapi.com/v1/current.json", params={
                "key": self.weather_api_key,
                "q": self.location,
            })
            response.raise_for_status()

            # Parse the response and publish the event
            data = response.json()
            event = Event(json.dumps(data).encode("utf-8"), mimetype="application/json")
            await self.ensign.publish(self.topic, event, on_ack=self.print_ack, on_nack=self.print_nack)

            # Wait 60 seconds in between requests
            await asyncio.sleep(60)
    
    def run_forever(self):
        """
        Run the publisher forever.
        """
        asyncio.run(self.recv_and_publish())

if __name__ == "__main__":
    # Create a publisher
    topic = os.environ.get("WEATHER_TOPIC")
    location = os.environ.get("WEATHER_LOCATION")
    publisher = WeatherPublisher(topic=topic, location=location)

    # Run the publisher forever
    publisher.run_forever()