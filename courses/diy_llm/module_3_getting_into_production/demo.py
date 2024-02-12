import os
import asyncio
import streamlit as st
from pyensign.ensign import Ensign
from pyensign.ml.dataframe import DataFrame
from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification

def handle_input(sent):
    st.text_area("Enter a movie review", key="input")
    if st.button("Predict Sentiment"):
        input_text = st.session_state.input
        result = sent(input_text)
        st.write("Sentiment:", result[0]["label"])
        st.write("Confidence:", result[0]["score"])

async def app(ensign):
    st.title("Movie Review Sentiment Analysis")

    # Read the latest model from Ensign + Hugging Face
    query = "SELECT * FROM sentiment-models"
    cursor = await ensign.query(query)
    models = await DataFrame.from_events(cursor)
    model_path = models.iloc[-1]["model_path"]
    model_version = models.iloc[-1]["model_version"]
    st.write("Using model {} @ {}".format(model_path, model_version))
    model = AutoModelForSequenceClassification.from_pretrained(model_path)
    tokenizer = AutoTokenizer.from_pretrained(model_path)
    sent = pipeline("sentiment-analysis", model=model, tokenizer=tokenizer)
    handle_input(sent)

if __name__ == "__main__":
    ensign = Ensign(client_id=os.environ["ENSIGN_CLIENT_ID"], client_secret=os.environ["ENSIGN_CLIENT_SECRET"])
    asyncio.run(app(ensign))