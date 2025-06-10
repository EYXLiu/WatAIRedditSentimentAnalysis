# Wat AI Reddit Scraping Bot
A reddit post and subreddit scrapping bot that performs sentiment analysis on two league teams. <br/>
Uses Praw for scraping and duckdb for storage accross files. 

# Overview
`scrapPost.py` is a scraper specifically for posts. The URL is inputted at the top and it scraps the post + all comments in a thread like manner. 
It saves the comments in a string with all the previous comments so that context is not lost. <br/>
`scrapReddit.py` is a scraper for subreddits. It scrolls through inputted subreddits and looks for the key term. It makes sure the post has not be saved before and then parses
down the comment tree, as above, and saves all the comments. <br/>
`sentimentAnalysis.py` is a model based on sequence classification, eg. sentiment analysis towards key words. <br/>
`classificationAnalysis.py` is a model based on zero-shot-classification, eg. whichever string the post is more related towards. Then sentiment analysis is performed 
to determine sentinment. <br/>
`findings.ipynb` stores the output of the model, displayed using matplotlib, and an analysis towards the underperformance and the reasons. Improvements are suggested. 
