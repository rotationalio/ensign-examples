import os
import json
import asyncio

from pyensign import nack
from pyensign.ensign import Ensign

class WeatherSubscriber:
    def __init__(self, topic="current-weather"):
        """
        Create a subscriber that subscribes to the weather topic.  
        """
        self.topic = topic
        self.ensign = Ensign()

    async def subscribe(self):
        """
        Subscribe to weather events on the topic.
        """

        # Ensure the topic exists
        await self.ensign.ensure_topic_exists(self.topic)

        async for event in self.ensign.subscribe(self.topic):
            # Attempt to decode the JSON event
            try:
                data = json.loads(event.data.decode("utf-8"))
            except json.JSONDecodeError as e:
                print("Error decoding event data: {}".format(e))
                await event.nack(nack.UnknownType)
                continue

            print("Received weather event for {} at {} local time".format(data["location"]["name"], data["location"]["localtime"]))
            print("Current temperature is {}°F".format(data["current"]["temp_f"]))
            print("Feels like {}°F".format(data["current"]["feelslike_f"]))
            print("Humidity is {}%".format(data["current"]["humidity"]))
            print("Wind is {} mph from {}".format(data["current"]["wind_mph"], data["current"]["wind_dir"]))
            print("Visibility is {} miles".format(data["current"]["vis_miles"]))
            print("Precipitation is {} inches".format(data["current"]["precip_in"]))

            # Success! Acknowledge the event
            await event.ack()

    def run_forever(self):
        """
        Run the subscriber forever.
        """
        asyncio.run(self.subscribe())

if __name__ == "__main__":
    topic = os.environ.get("WEATHER_TOPIC")
    subscriber = WeatherSubscriber(topic)
    subscriber.run_forever()