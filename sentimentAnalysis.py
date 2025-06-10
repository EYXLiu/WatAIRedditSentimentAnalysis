from transformers import pipeline
import duckdb

from transformers import AutoTokenizer, AutoModelForSequenceClassification

import numpy as np
import torch
import torch.nn.functional as F

import  json

con = duckdb.connect('redditposts.duckdb')

con.execute("""        
    DROP TABLE IF EXISTS sentiment;
    
    CREATE TABLE IF NOT EXISTS sentiment (
        comment TEXT
    )
""")

tokenizer = AutoTokenizer.from_pretrained("yangheng/deberta-v3-base-absa-v1.1", use_fast=False)
model = AutoModelForSequenceClassification.from_pretrained("yangheng/deberta-v3-base-absa-v1.1")

classifier = pipeline("text-classification", model=model, tokenizer=tokenizer)

MAX_TOKENS = 128
labels = ['Negative', 'Neutral', 'Positive']
targets = ['T1', 'Geng']

database = con.execute("SELECT * FROM posts").fetchall()
data = [x[3] for x in database]

def aggregate(text):
    results = {}
    for target in targets:
        chunks = [text[i:i+MAX_TOKENS] for i in range(0, len(text), MAX_TOKENS)]
        scores = []
        
        for chunk in chunks:
            s = classifier(chunk, text_pair=target)[0]
            adjusted = s['score'] if s['label'].lower() == 'positive' else 0 if s['label'].lower() == 'neutral' else 0 - s['score']
            scores.append(adjusted)
        
        avg = np.mean(scores)
        label = "POSITIVE" if avg >= 0.3 else "NEGATIVE" if avg <= -0.3 else "NEUTRAL"
        
        results[target] = {"label": label, "score": avg, "text": text}
    
    return results

result = []
for text in data:
    result.append(aggregate(text))
    
for i in result:
    con.execute("""
                INSERT INTO sentiment (comment)
                VALUES (?)
                """, (
                    [json.dumps(i)]
                ))
    print(i)