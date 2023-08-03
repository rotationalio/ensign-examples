# everything that is part of the standard library
import json
import asyncio
import warnings

# everything that has to get pip or conda installed
import spacy
import msgpack
from bs4 import BeautifulSoup
from textblob import TextBlob
from pyensign.ensign import Ensign
from pyensign.api.v1beta1.ensign_pb2 import Nack

# local package components
from config import ENSIGN_CLIENT_ID, ENSIGN_CLIENT_SECRET



# TODO in Python>3.10
# TODO need to ignore DeprecationWarning: There is no current event loop
warnings.filterwarnings("ignore")

class BaleenSubscriber:
    """
    Implementing an event-driven Natural Language Processing tool that
    does streaming HTML parsing, entity extraction, and sentiment analysis
    """
    def __init__(self, topic="documents", client_id=ENSIGN_CLIENT_ID, client_secret=ENSIGN_CLIENT_SECRET):
        """
        Initilaize the BaleenSubscriber, which will allow a data consumer
        to subscribe to the topic that the publisher is pushing articles
        """

        self.topic = topic
        self.ensign = Ensign(
            client_id=client_id,
            client_secret=client_secret
        )
        self.NER = spacy.load('en_core_web_sm')

    def run(self):
        """
        Run the subscriber forever.
        """
        asyncio.run(self.subscribe())
        
    async def handle_event(self,event):
        """
        Decode and ack the event.

        ----------------
        Unpacking of the event message and working on the article content for
        NLP Magic
        """
        try:
            data = msgpack.unpackb(event.data)
        except json.JSONDecodeError:
            print("Received invalid JSON in event payload:", event.data)
            await event.nack(Nack.Code.UNKNOWN_TYPE)
            return

        # Parsing the content using BeautifulSoup
        soup = BeautifulSoup(data[b'content'], 'html.parser')
        # Finding all the 'p' tags in the parsed content
        paras = soup.find_all('p')
        score = []
        ner_dict = {}
        for para in paras:
            text = TextBlob(para.get_text())
            score.append(text.sentiment.polarity)
            ner_text = self.NER(str(para.get_text()))
            for word in ner_text.ents:
                if word.label_ in ner_dict.keys():
                    if word.text not in ner_dict[word.label_]:
                        ner_dict[word.label_].append(word.text)
                else :
                    ner_dict[word.label_] = [word.text]

        print("\nSentiment Average Score : ", sum(score) / len(score))
        print("\n------------------------------\n")
        print("Named Entities : \n",json.dumps(
                ner_dict,
                sort_keys=True,
                indent=4,
                separators=(',', ': ')
                )
              )
        await event.ack()

    async def subscribe(self):
       """
       Subscribe to the article and parse the events.
       """
       id = await self.ensign.topic_id(self.topic)
       async for event in self.ensign.subscribe(id):
            await self.handle_event(event)
       await asyncio.Future()

if __name__ == "__main__":
    subscriber = BaleenSubscriber()
    subscriber.run()