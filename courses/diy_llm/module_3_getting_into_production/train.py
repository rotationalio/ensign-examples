import os
import json
import evaluate
import numpy as np
from pyensign.events import Event
from pyensign.ensign import Ensign
from transformers import TrainingArguments, Trainer, AutoTokenizer, DataCollatorWithPadding, AutoModelForSequenceClassification, pipeline

from dataset import DataFrameSet, EnsignLoader

class Trainer:
    """
    Class for training a model with the transformers library and PyTorch.
    """

    def __init__(self, model_topic="sentiment-models", ensign_creds=None, tokenizer="distilbert-base-uncased", model="distilbert-base-uncased", eval_metric="accuracy", output_dir="results", num_epochs=2, version="v0.1.0"):
        if isinstance(tokenizer, str):
            self.tokenizer = AutoTokenizer.from_pretrained(tokenizer)
        else:
            self.tokenizer = tokenizer
        self.data_collator = DataCollatorWithPadding(tokenizer=self.tokenizer)
        self.accuracy = evaluate.load(eval_metric)
        id2label = {0: "negative", 1: "positive"}
        label2id = {"negative": 0, "positive": 1}
        self.model = AutoModelForSequenceClassification.from_pretrained(model, num_labels=2, id2label=id2label, label2id=label2id)
        self.output_dir = output_dir
        self.model_topic = model_topic
        self.train_args = {
            "output_dir": self.output_dir,
            "learning_rate": 2e-5,
            "per_device_train_batch_size": 16,
            "per_device_eval_batch_size": 16,
            "num_train_epochs": num_epochs,
            "weight_decay": 0.01,
            "evaluation_strategy": "epoch",
            "save_strategy": "epoch",
            "load_best_model_at_end": True,
        }
        self.training_args = TrainingArguments(**self.train_args)
        self.ensign = Ensign(cred_path=ensign_creds)
        self.loader = EnsignLoader(self.ensign)
        self.train_set = None
        self.test_set = None
        self.trainer = None
        self.version = version

    def _compute_metrics(self, eval_pred):
        preds, labels = eval_pred
        preds = np.argmax(preds, axis=1)
        return self.accuracy.compute(predictions=preds, references=labels)
    
    async def load_dataset(self, topic):
        df = await self.loader.load_all(topic)
        self.train_set = DataFrameSet(df[df["split"] == "train"], tokenizer=self.tokenizer)
        self.test_set = DataFrameSet(df[df["split"] == "test"], tokenizer=self.tokenizer)
        self.train_set.preprocess()
        self.test_set.preprocess()

    def train(self):
        self.trainer = Trainer(
            model=self.model,
            args=self.training_args,
            train_dataset=self.train_set,
            eval_dataset=self.test_set,
            tokenizer=self.tokenizer,
            data_collator=self.data_collator,
            compute_metrics=self._compute_metrics
        )
        self.trainer.train()

    async def publish_latest_model(self, hub_username, hub_token, model_name="movie-reviews-sentiment", eval=True):
        latest = None
        checkpoint = 0
        for name in os.listdir(self.output_dir):
            num = int(name.split("-")[-1])
            if num > checkpoint:
                checkpoint = num
                latest = name
        model_path = os.path.join(self.output_dir, latest)
        model = AutoModelForSequenceClassification.from_pretrained(model_path)
        tokenizer = AutoTokenizer.from_pretrained(model_path)

        hub_path = f"{hub_username}/{model_name}"
        data = {
            "model_host": "huggingface.co",
            "model_path": hub_path,
            "model_version": self.version,
            "training_args": self.train_args,
            "trained_at": os.path.getmtime(model_path)
        }
        if eval:
            sent = pipeline("sentiment-analysis", model=model, tokenizer=self.tokenizer, truncation=True)
            preds = sent(self.test_set.features())
            labels = self.test_set.labels()
            data["eval_accuracy"] = self.accuracy.compute(predictions=preds, references=labels)
        event = Event(
            json.dumps(data).encode("utf-8"),
            mimetype="application/json",
            schema_name="transformer-model",
            schema_version=self.version,
        )

        model.push_to_hub(model_name, token=hub_token, revision=self.version)
        tokenizer.push_to_hub(model_name, token=hub_token, revision=self.version)
        await self.ensign.publish(self.model_topic, event)