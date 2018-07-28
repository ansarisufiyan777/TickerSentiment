from tweepy import Stream
from tweepy.streaming import StreamListener

# This is a basic listener that just prints received tweets to stdout.
from sentiment.sentiment import SentimentPolarity
from stocks.stocks_data import StocksData
from twitter.twitter_auth import TwitterAuth
from util.config import Config


class TwitterStreamer(StreamListener):
    logger = Config.get_logger()

    def on_data(self, data):
        print('Twitteer data in %s', data)
        SentimentPolarity.do_work(data, 'Twitter')
        return True

    def on_error(self, status):
        print(status)


if __name__ == '__main__':
    # This handles Twitter authetification and the connection to Twitter Streaming API
    l = TwitterStreamer()

    stream = Stream(TwitterAuth.get_auth(), l)

    # This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
    json_object = StocksData.fetch_ticker_list()
    tickers = []
    for (attribute, value) in json_object.items():
        print(attribute, value)
        tickers.append(attribute)
    stream.filter(track=attribute, languages=['en'])
