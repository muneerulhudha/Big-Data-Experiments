#!/usr/bin/env python
# encoding: utf-8

import tweepy #https://github.com/tweepy/tweepy
import csv
import json

from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener


#Twitter API credentials
CONSUMER_KEY = ""
CONSUMER_SECRET = ""
ACCESS_KEY = "-"
ACCESS_sECRET = ""

count = 0;

class MyListener(StreamListener):
    def on_data(self, data):
        jsonData=json.loads(data)
        text1=jsonData['text']
        text2=jsonData['entities']['hashtags']        
        for hashtag in text2:
            text2=hashtag['text']
            print text2,":",text1
            f1=open('./tweetFile_superBOWL6.csv', 'ab')            
            a = csv.writer(f1, delimiter=',')
            a.writerow(["UJG123469",str(text2.encode('utf-8')),str(text1.encode('utf-8'))])          
        return StreamListener.on_data(self, data)

    def on_error(self, status):
        print(status)
        return True

def Collect_Tweets(): 
    #Authorize and Initialize Tweepy
    AUTH = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    AUTH.set_access_token(ACCESS_KEY, ACCESS_sECRET)     
    twitter_stream = Stream(AUTH, MyListener())
    twitter_stream.filter(track=["teamCap"])
      
if __name__ == '__main__':
    Collect_Tweets()