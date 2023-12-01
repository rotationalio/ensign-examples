import os
import json
import asyncio

from pyensign.events import Event
from pyensign.ensign import Ensign
from aiohttp import ClientSession, BasicAuth
from python_opensky import OpenSky, BoundingBox

class FlightsPublisher:
    def __init__(
        self,
        topic="flight-positions",
        min_latitude=-66,
        max_latitude=49,
        min_longitude=-124,
        max_longitude=24,
        interval=60,
    ):
        self.topic = topic
        self.bounding_box = BoundingBox(
            min_latitude=min_latitude,
            max_latitude=max_latitude,
            min_longitude=min_longitude,
            max_longitude=max_longitude,
        )
        self.interval = interval
        self.ensign = Ensign()
        self.opensky_creds = BasicAuth(
            login=os.environ['OPENSKY_USERNAME'],
            password=os.environ['OPENSKY_PASSWORD']
        )

    def vectors_to_events(self, vectors):
        for vector in vectors:
            data = {
                "icao24": vector.icao24,
                "callsign": vector.callsign,
                "origin_country": vector.origin_country,
                "time_position": vector.time_position,
                "last_contact": vector.last_contact,
                "longitude": vector.longitude,
                "latitude": vector.latitude,
                "geo_altitude": vector.geo_altitude,
                "on_ground": vector.on_ground,
                "velocity": vector.velocity,
                "true_track": vector.true_track,
                "vertical_rate": vector.vertical_rate,
                "sensors": vector.sensors,
                "barometric_altitude": vector.barometric_altitude,
                "transponder_code": vector.transponder_code,
                "special_purpose_indicator": vector.special_purpose_indicator,
                "position_source": vector.position_source,
                "category": vector.category,
            }
            yield Event(
                data=json.dumps(data).encode("utf-8"),
                mimetype="application/json",
                schema_name="FlightVector",
                schema_version="0.1.0",
            )
    
    async def recv_and_publish(self):
        # Create topic if it doesn't exist
        await self.ensign.ensure_topic_exists(self.topic)

        async with ClientSession() as session:
            async with OpenSky(session=session) as opensky:
                await opensky.authenticate(self.opensky_creds)

                while True:
                    # Call the OpenSky API to get flight vectors in the bounding box
                    try:
                        response = await opensky.get_states(
                            bounding_box=self.bounding_box
                        )
                    except Exception as e:
                        print(e)
                        await asyncio.sleep(self.interval)
                        continue

                    # Publish each flight vector to Ensign
                    for event in self.vectors_to_events(response.states):
                        await self.ensign.publish(
                            self.topic,
                            event,
                            on_ack=self.print_ack,
                            on_nack=self.print_nack
                        )

                    await asyncio.sleep(self.interval)

    async def print_ack(self, ack):
        print(f"Event committed with ack: {ack}")

    async def print_nack(self, nack):
        print(f"Event was not committed with error {nack.code}: {nack.error}")

    def run(self):
        asyncio.run(self.recv_and_publish())

if __name__ == "__main__":
    publisher = FlightsPublisher()
    publisher.run()