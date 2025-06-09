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

post_url = "https://www.reddit.com/r/leagueoflegends/comments/1gdeoa7/t1_vs_geng_2024_world_championship_semifinal/"

def parse(url):
    submission = reddit.submission(url=url)
    cursor = con.execute("SELECT 1 FROM postNames WHERE title = ? LIMIT 1", (submission.title, ))
    exists = cursor.fetchone() is not None
    if (exists):
        return
    
    curr = f"Previous {submission.title}"
    
    con.execute("""
                    INSERT INTO postNames (id, title)
                    VALUES (?, ?)
                    """, (
                        submission.id,
                        submission.title,
                    ))
    
    submission.comments.replace_more(limit=None)
    
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
    parse(post_url)
    print(con.execute("SELECT * FROM posts LIMIT 1").fetchall())

if __name__ == "__main__":
    main()