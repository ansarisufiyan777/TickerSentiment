from tweepy import OAuthHandler


# This is a basic listener that just prints received tweets to stdout.
from util.config import Config


class TwitterAuth:

    @staticmethod
    def get_auth():
        consumer_key, consumer_secret, access_token, access_token_secret = Config.get_twitter_config()
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        return auth
