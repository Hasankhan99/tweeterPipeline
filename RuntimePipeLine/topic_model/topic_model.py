import spacy
from classy_classification import classyClassifier
import pickle


class topicModel:
    def __init__(self) -> None:
        self.data = pickle.load(
            open("D:\kwantx\Twitter_Project\models\RuntimePipeLine\\topic_model\\topicModel.pkl", "rb"))
        self.classifier = classyClassifier(data=self.data)

    def predictTopic(self, text):
        return self.classifier(text)
