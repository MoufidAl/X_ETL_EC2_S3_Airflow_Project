import tweepy
import pandas as pd 
import json
from datetime import datetime
import s3fs

## X won't allow for tweet data without v2 API endpoints.


def run_x_etl():


    access_key = "TKh..."
    access_secret_key = "N...."
    customer_key = "1...."
    customer_secret_key = "K...."

    # X authentication 

    auth = tweepy.OAuthHandler(access_key,access_secret_key)

    auth.set_access_token(customer_key, customer_secret_key)

    # Creating the X API object

    api = tweepy.API(auth)

    tweets = api.user_timeline(screen_name = '@NASA',
                            # count refers to the number of tweets I want to extract. - max is 200
                            count = 200,
                            # Include Retweets: If I want to include any retweets made by the account.
                            include_rts = False,
                            # extended refers to extracting not only the first 140 words but the entire tweet.
                            tweet_mode = 'extended')


    tweet_list = []

    for tweet in tweets:
        text = tweet._json['full_text']

        cleaned_tweet = {'user': tweet.user.screen_name,
                        'text': text,
                        'favorited_count': tweet.favorite_count,
                        'retweeted_count': tweet.retweet_count,
                        'created_time': tweet.created_at}
        
        tweet_list.append(cleaned_tweet)

    # Creating the PD dataframe to store the data

    df = pd.DataFrame(tweet_list)

    # Sending it to a CSV file within a S3 bucket

    df.to_csv("s3://BUCKET NAME/CSV FILE NAME.csv")


