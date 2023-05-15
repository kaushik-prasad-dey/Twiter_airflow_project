import tweepy
import pandas as pd 
import json
from datetime import datetime
import s3fs 

def run_twitter_etl_kaushik():

    access_key = "QV8xf55b0zEim8Q5CcxszrKDC" 
    access_secret = "0knuat0CNBJtTYomBCtFvfC8YSZwb46icn6bG5VyvvW8gPXz7e" 
    consumer_key = "1647040449121759234-FE3nhJAWmMDioKn3gFQBCAmmpe7Yzm"
    consumer_secret = "oYsNnZKTYuSNSxeMEnp2MTB0N6WNMarKLqwOw8bwGrJ5y"


    # Twitter authentication
    auth = tweepy.OAuthHandler(access_key, access_secret)   
    auth.set_access_token(consumer_key, consumer_secret) 

    # # # Creating an API object 
    api = tweepy.API(auth)
    tweets = api.user_timeline(screen_name='@narendramodi', 
                            # 200 is the maximum allowed count
                            count=200,
                            include_rts = False,
                            # Necessary to keep full_text 
                            # otherwise only the first 140 words are extracted
                            tweet_mode = 'extended'
                            )

    list = []
    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {"user": tweet.user.screen_name,
                        'text' : text,
                        'favorite_count' : tweet.favorite_count,
                        'retweet_count' : tweet.retweet_count,
                        'created_at' : tweet.created_at}
        
        list.append(refined_tweet)

    df = pd.DataFrame(list)
    df.to_csv('s3://kaushik-airflow-bucket-logs/refined_tweets_data.csv')
