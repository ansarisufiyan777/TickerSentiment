import logging

from six.moves import configparser


class Config:

    @staticmethod
    def get_logger():
        # importing module

        # Create and configure logger
        logging.basicConfig(filename="TickerSentiment.log",
                            format='%(asctime)s %(message)s',
                            level=logging.INFO,
                            filemode='w')

        # Creating an object
        logger = logging.getLogger()

        # Setting the threshold of logger to DEBUG
        logger.setLevel(logging.DEBUG)

        return logger

    @staticmethod
    def get_config():
        config = configparser.ConfigParser()
        config.read('../setup.cfg')
        return config;

    @staticmethod
    def get_twitter_config():
        c = Config.get_config()
        consumer_key = c.get('TweetStreaming', 'consumer_key')
        consumer_secret = c.get('TweetStreaming', 'consumer_secret')
        access_token = c.get('TweetStreaming', 'access_token')
        access_token_secret = c.get('TweetStreaming', 'access_token_secret')
        return consumer_key, consumer_secret, access_token, access_token_secret

    @staticmethod
    def get_aws_config():
        c = Config.get_config()
        end_point = c.get('Elasticsearch', 'end_point')
        return end_point

    @staticmethod
    def get_vantage_url(ticker):
        c = Config.get_config()
        return c.get('AlphaVantage', 'url') % ticker
