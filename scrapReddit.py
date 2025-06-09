import praw

from dotenv import load_dotenv
import os
import json

from datetime import datetime, UTC
import time

import duckdb
import pandas as pd

load_dotenv()

reddit = praw.Reddit(
    client_id=os.getenv("client_id"),
    client_secret=os.getenv("client_secret"),
    user_agent=os.getenv("user_agent"),
)

con = duckdb.connect(database="redditposts.duckdb")

con.execute("""
    CREATE TABLE IF NOT EXISTS posts (
        id TEXT,
        title TEXT,
        author TEXT,
        body TEXT,
        timestamp DOUBLE,
    )
""")

con.execute("""
    CREATE TABLE IF NOT EXISTS postNames (
        id TEXT,
        title TEXT
    )
""")

one_week_ago = time.time() - 7 * 24 * 60 * 60

def parse():
    for submission in reddit.subreddit("PedroPeepos+leagueoflegends+sktt1").search("t1", limit=10):
        cursor = con.execute("SELECT 1 FROM postNames WHERE title = ? LIMIT 1", (submission.title, ))
        exists = cursor.fetchone() is not None
        if (exists):
            continue
        
        submission.comments.replace_more(limit=None)
        
        curr = f"Previous: {submission.title} {submission.selftext}"
        
        con.execute("""
                    INSERT INTO posts (id, title, author, body, timestamp)
                    VALUES (?, ?, ?, ?, ?)
                    """, (
                        submission.id,
                        submission.title,
                        str(submission.author), 
                        submission.selftext,
                        submission.created_utc,
                    ))
        con.execute("""
                    INSERT INTO postNames (id, title)
                    VALUES (?, ?)
                    """, (
                        submission.id,
                        submission.title,
                    ))
        
        for comment in submission.comments:
            def collect_comments(c, prev):
                con.execute("""
                    INSERT INTO posts (id, title, author, body, timestamp)
                    VALUES (?, ?, ?, ?, ?)
                    """, (
                        c.id,
                        submission.title,
                        str(c.author), 
                        f"{prev} {c.body}",
                        c.created_utc,
                    ))
                new_curr = f"{prev} {c.body}"
                for reply in c.replies:
                    collect_comments(reply, new_curr)
            collect_comments(comment, curr)

def main():
    parse()
    print(con.execute("SELECT * FROM posts LIMIT 1").fetchall())

if __name__ == "__main__":
    main()