import json
import pickle
import asyncio
from pyensign.events import Event
from pyensign.ensign import Ensign
from river import compose, linear_model, preprocessing

class FlightsSubscriber:
    def __init__(self, flights_topic="flight-positions", models_topic="arrival-models", predictions_topic="arrival-predictions"):
        self.flights_topic = flights_topic
        self.models_topic = models_topic
        self.predictions_topic = predictions_topic
        self.ensign = Ensign()
        self.model = compose.Pipeline(
            preprocessing.StandardScaler(),
            linear_model.LinearRegression()
        )

    def predict_velocity(self, vector):
        # Get the features for training
        features = {
            "barometric_altitude": vector.barometric_altitude,
            "latitude": vector.latitude,
            "longitude": vector.longitude,
            "true_track": vector.true_track,
        }

        # Obtain the prior prediction and update the model
        velocity_pred = self.model.predict_one(features)
        self.model.learn_one(features, vector.velocity)

        # Return the true value and the prediction
        return vector.velocity, velocity_pred
        
    async def run(self):
        self.callsign = ""

        async for event in self.ensign.subscribe(self.flights_topic):
            # Decode the event data into the vector
            vector = json.loads(event.data)

            # Look for the callsign which indicates the flight we are interested in
            callsign = vector["callsign"].strip()
            if self.callsign == "":
                self.callsign = callsign
            
            if self.callsign == callsign:
                print("received flight vector", vector)

                # Get the true velocity and the prediction
                velocity, velocity_pred = self.predict_velocity(vector)

                # Publish the prediction along with the true value
                pred_data = {
                    "predicted_velocity": velocity_pred,
                    "true_velocity": velocity,
                }
                pred_event = Event(data=json.dumps(pred_data).encode("utf-8"), mimetype="application/json", schema_name="VelocityPrediction", schema_version="0.1.0")
                model_event = Event(data=pickle.dumps(self.model), mimetype="application/python-pickle",  schema_name="VelocityModel", schema_version="0.1.0")

                await self.ensign.publish(self.predictions_topic, pred_event)
                await self.ensign.publish(self.models_topic, model_event)

                print("published prediction event", pred_event)
                print("published model event", model_event)

if __name__ == "__main__":
    subscriber = FlightsSubscriber()
    asyncio.run(subscriber.run())