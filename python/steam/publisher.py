import os
import json
import asyncio
from datetime import datetime

from pyensign.ensign import Ensign
from pyensign.events import Event
from steam.webapi import WebAPI

async def handle_ack(ack):
    ts = datetime.fromtimestamp(ack.committed.seconds + ack.committed.nanos / 1e9)
    print(f"Event committed at {ts}")

async def handle_nack(nack):
    print(f"Could not commit event {nack.id} with error {nack.code}: {nack.error}")

class ActivityPublisher:
    """
    ActivityPublisher publishes the number of users playing a game on Steam. 
    """

    def __init__(self, app_id=730, topic='game_activity', interval=5):
        self.app_id = app_id
        self.topic = topic
        self.interval = interval
        self.api = WebAPI(key=os.environ['STEAM_API_KEY'])
        self.ensign = Ensign()

        if not self.app_id:
            raise ValueError('Game not found.')

    async def query_and_publish(self, app_id, interval):
        """
        Query the Steam API on an interval and publish evnts to Ensign.
        """

        while True:
            # Query the Steam API for the number of users playing a game.
            response = self.api.ISteamUserStats.GetNumberOfCurrentPlayers(appid=app_id)

            # Publish the number of users playing a game to Ensign.
            data = {
                'app_id': app_id,
                'player_count': response['response']['player_count'],
            }
            print("Pubishing activity event: ", data)
            event = Event(json.dumps(data).encode('utf-8'), mimetype="application/json")
            await self.ensign.publish(self.topic, event, ack_callback=handle_ack, nack_callback=handle_nack)

            # Wait until the next interval.
            await asyncio.sleep(interval)

    def run(self):
        """
        Run the publisher forever.
        """
        
        asyncio.get_event_loop().run_until_complete(self.query_and_publish(self.app_id, self.interval))

if __name__ == '__main__':
    publisher = ActivityPublisher()
    publisher.run()
