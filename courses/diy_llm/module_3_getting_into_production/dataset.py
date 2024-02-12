from torch.utils.data import Dataset
from transformers import AutoTokenizer
from pyensign.ml.dataframe import DataFrame

class DataFrameSet(Dataset):
    """
    DataFrameSet represents a training dataset stored in a DataFrame. This class is
    responsible for preprocessing and passing the data to PyTorch for training. We must
    implement the __len__ and __getitem__ methods to make this compatible with PyTorch.
    """
    
    def __init__(self, dataframe, tokenizer="distilbert-base-uncased", feature_key="text", label_key="label"):
        self.feature_key = feature_key
        self.label_key = label_key
        self.dataframe = dataframe
        if isinstance(tokenizer, str):
            self.tokenizer = AutoTokenizer.from_pretrained(tokenizer)
        else:
            self.tokenizer = tokenizer

    def _tokenize(self, df):
        return self.tokenizer(df[self.feature_key], truncation=True)

    def preprocess(self):
        self.dataframe["tokens"] = self.dataframe.apply(self._tokenize, axis=1)

    def features(self):
        return self.dataframe[self.feature_key]
    
    def labels(self):
        return self.dataframe[self.label_key]

    def __len__(self):
        return len(self.dataframe)
    
    def __getitem__(self, idx):
        return {
            "input_ids": self.dataframe["tokens"].iloc[idx]["input_ids"],
            "attention_mask": self.dataframe["tokens"].iloc[idx]["attention_mask"],
            "labels": self.dataframe[self.label_key].iloc[idx]
        }

class EnsignLoader:
    """
    EnsignLoader is a utility class for loading datasets from Ensign.
    """

    def __init__(self, ensign):
        self.ensign = ensign

    async def load_all(self, topic):
        query = f"SELECT * FROM {topic}"
        cursor = await self.ensign.query(query)
        return await DataFrame.from_events(cursor)

