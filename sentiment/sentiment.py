from __future__ import print_function

import json

from dateutil.parser import parse

from datetime import datetime

from textblob import TextBlob

from aws.aws_message import AwsMessages


class SentimentPolarity:
    @classmethod
    def do_work(cls, data):
        msg = cls.get_json(data)
        text = msg["text"]
        try:
            testimonial = TextBlob(text)
            polarity = testimonial.sentiment.polarity
            if polarity > 0:
                emotion = "positive"
            elif polarity < 0:
                emotion = "negative"
            else:
                emotion = "neutral"
            print(emotion)

        except Exception as e:
            print("ERROR: " + str(e))
            emotion = "neutral"
            print(emotion)

        msg["sentiment"] = emotion
        AwsMessages.upload_msg(msg,AwsMessages.twitter_index,AwsMessages.twitter_mapping)

    @classmethod
    def get_json(cls, message):
        msg = json.loads(message)
        timezone_offset = datetime.utcnow() - datetime.now()
        dt = parse(msg['created_at'])
        local_created_time = dt - timezone_offset;
        tweet = {
            'name': msg['user']['screen_name'],
            'time': local_created_time.strftime("%Y/%m/%d %H:%M:%S"),
            'text': msg['text'],
            'profile_image_url': msg['user']['profile_image_url'],
            'sentiment': '',
            message: message
        }
        return tweet;
