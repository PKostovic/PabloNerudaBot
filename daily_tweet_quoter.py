import tweepy
import yaml
from pymongo import MongoClient

# Import credentials
with open('/home/pavle/projects/twitterbot/secrets/twitter_secrets.yaml') as credentials_file:
    credentials = yaml.load(credentials_file, Loader=yaml.FullLoader)

# Authenticate to Twitter
auth = tweepy.OAuthHandler(credentials["api_key"], credentials["api_key_secret"]) 
auth.set_access_token(credentials["access_token"], credentials["access_token_secret"])

# Create API object
api = tweepy.API(auth)

# Connect to the Mongo instance, grab the Neruda db, and the Quotes collection
mongo_client = MongoClient('mongodb://localhost:27017/')
mongo_db = mongo_client['neruda_quotes']
quote_collection = mongo_db['neruda_quote_collection']

# Grab a quote at random
unpublished_quotes = quote_collection.find({'author':'Pablo Neruda', 'recently_used': False})
print(len(list(unpublished_quotes)))

new_attempt = quote_collection.aggregate([{'$sample':{'size':1}}])
print(list(new_attempt))



# TODO: check if the quote is >280 characters, and if it is, figure out how to break it up into multiple quotes
# print(len(selected_quote['quote']))

# Tweet out the quote!
# api.update_status(selected_quote['quote'])
