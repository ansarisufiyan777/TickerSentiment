from nsetools import Nse

from aws.aws_message import AwsMessage
from util.config import Config


class StocksData:
    logger = Config.get_logger()

    def __init__(self):
        pass

    @classmethod
    def fetch_stocks(cls):
        nse = Nse()
        print(nse)
        json_data = StocksData.fetch_ticker_list()
        count = 0
        for (attribute, value) in json_data.items():
            if count == 0:
                count += 1
                continue
            print(attribute, value)
            q = nse.get_quote(attribute)
            AwsMessage.upload_msg(q, AwsMessage.stock_address)
            count += 1

    @classmethod
    def fetch_ticker_list(cls):
        nse = Nse()
        index_codes = nse.get_stock_codes()
        # pprint(index_codes)
        return index_codes


if __name__ == '__main__':
    StocksData.fetch_stocks()
