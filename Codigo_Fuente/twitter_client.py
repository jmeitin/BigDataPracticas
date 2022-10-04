import socket
import sys
import requests
import requests_oauthlib
import json

# Replace the values below with yours
CONSUMER_KEY = 'Your API Key'
CONSUMER_SECRET = 'Your API Key Secret'
ACCESS_TOKEN = 'Your Access Token'
ACCESS_SECRET = 'Your Access Token Secret'
my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET)

def send_tweets_to_spark(http_resp, tcp_conn):
    for line in http_resp.iter_lines():
        try:
            full_tweet = json.loads(line)
            tweet_text = full_tweet['text']
            print("Tweet Text: " + tweet_text)
            print ("------------------------------------------")
            #for x in full_tweet['entities']['hashtags']:
            #    print(x['text'])
            tcp_conn.send(bytes(tweet_text + '\n', "utf-8"))
        except Exception as e:
            print("Error: " + str(e))

def get_tweets():
    query_url = 'https://stream.twitter.com/1.1/statuses/filter.json?language=es&locations=-10,36,4,44'
    response = requests.get(query_url, auth=my_auth, stream=True)
    print(query_url, response)
    return response

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(("localhost", 9009))
s.listen(1)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print("Connected! Starting getting tweets...")
resp = get_tweets()
send_tweets_to_spark(resp, conn)
