from unicodedata import name
from .finberT.finbert import predict
import os
from transformers import AutoModelForSequenceClassification


def finbert_predict(text):
    project_dir = os.path.dirname(os.path.abspath(__file__))
    model_path = os.path.join(
        project_dir, 'models/classifier_model/finbert-sentiment')
    model = AutoModelForSequenceClassification.from_pretrained(
        model_path, num_labels=3, cache_dir=None)
    result = predict(text, model)
    return result.to_json(orient='records')
