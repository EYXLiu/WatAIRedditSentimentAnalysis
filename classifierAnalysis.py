from transformers import AutoTokenizer, AutoModelForSequenceClassification
from transformers import pipeline

import numpy as np
import duckdb
import json

con = duckdb.connect('redditposts.duckdb')

pipe = pipeline("zero-shot-classification", model="MoritzLaurer/DeBERTa-v3-base-mnli-fever-anli")
sentiment = pipeline("sentiment-analysis", model="distilbert/distilbert-base-uncased-finetuned-sst-2-english")

pipe("The camera quality of this phone is amazing.", candidate_labels=["camera", "phone"])

con.execute("""        
    DROP TABLE IF EXISTS classifier;
    
    CREATE TABLE IF NOT EXISTS classifier (
        team TEXT,
        comment TEXT
    )
""")

MAX_TOKENS = 128
targets = ['T1', 'Geng']
comment_dict = {v: [] for v in targets}
sentiment_dict = {v: [] for v in targets}

database = con.execute("SELECT * FROM posts").fetchall()
data = [x[3] for x in database]

def aggregate(text):
    chunks = [text[i:i+MAX_TOKENS] for i in range(0, len(text), MAX_TOKENS)]
    scores = {v: 0 for v in targets}
    nums = {v: 0 for v in targets}
    
    for chunk in chunks:
        s = pipe(chunk, candidate_labels=targets)
        scores[s['labels'][0]] += s['scores'][0]
        nums[s['labels'][0]] += 1
    
    top = max(nums, key=nums.get)
    
    val = {
        'text': text,
        'score': scores[top] / nums[top]
    }
    
    comment_dict[top].append(val)
    
def analysis(text, v):
    s = sentiment(text['text'])[0]
    val = {
        'text': text['text'],
        'relation': text['score'],
        'sentiment': s['label'],
        'score': s['score']
    }
    sentiment_dict[v].append(val)
    
for text in data:
    aggregate(text)

for v in targets:
    for text in comment_dict[v]:
        analysis(text, v)

for v in targets:
    for comment in sentiment_dict[v]:
        con.execute("""
                    INSERT INTO classifier (team, comment)
                    VALUES (?, ?)
                    """, (
                        [v, json.dumps(comment)]
                    ))