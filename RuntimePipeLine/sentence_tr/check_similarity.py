from sentence_transformers import SentenceTransformer, util
import pickle
# model = SentenceTransformer('training_2022-09-14_00-48-55')
import pandas as pd

class CheckSimilarity:
    def __init__(self):
        
        self.senteces = pickle.load(open('sentence_tr/data.pkl', 'rb'))
        self.model = SentenceTransformer('all-MiniLM-L6-v2')
        self.embedding_s= self.model.encode(self.senteces)


    def check_similarity(self, text):
        tweet=[]
        tweet.append(text)
        embedding_t = self.model.encode(tweet)
        embedding_s = self.embedding_s
        allsentence=[]
        for i in range(len(self.senteces)):
            cos_sim = util.cos_sim(embedding_s[i], embedding_t)
            allsentence.append([cos_sim,self.senteces[i],tweet[0]])
        all_sentence_combinations = sorted(allsentence, key=lambda x: x[0], reverse=True) 
        return all_sentence_combinations[0]   


# # def similarity_score(text):
# #     sentences = pickle.load(open('F:/Hasan Work/tweeter2/runtimePipeLine/sentence_tr/data.pkl', 'rb'))
# #     # sentences = list(sentences)
# #     tweet=[]
# #     tweet.append(text)



# #     #Encode all sentences
# #     embedding_s = model.encode(sentences)
# #     embedding_t = model.encode(tweet)

# #     #Compute cosine similarity between all pairs
# #     all_sentence_combinations = []
# #     for i in range(len(sentences)):
# #         cos_sim = util.cos_sim(embedding_s[i], embedding_t)
        
# #         all_sentence_combinations.append([float(cos_sim[0][0]), sentences[i],tweet[0]])

# #     #Sort list by the highest cosine similarity score
# #     all_sentence_combinations = sorted(all_sentence_combinations, key=lambda x: x[0], reverse=True)
# #     return all_sentence_combinations[0]

# print(similarity_score('Ivanka, Jared, Mnuchin, Pompeo, Barr, Giuliani, Manafort, Flynn, Stone.The trump orbit is/was full of criminals and traitors. We dont want that back.Seriously, VOTE.'))