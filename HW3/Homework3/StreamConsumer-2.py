from kafka import KafkaConsumer
import json
from elasticsearch import Elasticsearch
es = Elasticsearch()
from textblob import TextBlob
from nltk.sentiment.vader import SentimentIntensityAnalyzer
sid = SentimentIntensityAnalyzer()

hashtags = ["#vaccine", "#coronavirus"]

def main():
    '''
    main function initiates a kafka consumer, initialize the tweetdata database.
    Consumer consumes tweets from producer, extracts features, cleanses the tweet text,
    calculates sentiments and loads the data into elasticsearch
    '''
    # set-up a Kafka consumer
    consumer = KafkaConsumer("twitter")
    for msg in consumer:

        dict_data = json.loads(msg.value)
        tweet = TextBlob(dict_data["text"])
        print(tweet)
        senti = sid.polarity_scores(str(tweet))
        for k in senti:
            print('{0}:{1},'.format(k, senti[k]), end='\n')
        if senti["compound"] > 0:
            result = "pos"
        elif senti["compound"] < 0:
            result = "neg"
        else:
            result = "neu"
        print("result: " + result)
        s = json.dumps(dict_data, indent=4).lower()
        for tag in hashtags:
            if tag in s:
                print(tag)
                # add text and sentiment info to elasticsearch
                es.index(index="tweet",
                         doc_type="test-type",
                         body={"text": dict_data["text"],
                               "hashtag": tag,
                               "sentiment": result})
        print('\n------------------\n')

if __name__ == "__main__":
    main()