import requests

from aws.aws_message import AwsMessage
from stocks.stocks_data import StocksData
from util.config import Config


class Financials:

    @classmethod
    def fetch_financials(cls):
        url = Config.get_vantage_url('MSFT')
        json_object = StocksData.fetch_ticker_list()
        for (attribute, value) in json_object.items():
            print(attribute, value)
            r = requests.get(url=url)
            data = r.json()
            AwsMessage.upload_msg(data, AwsMessage.finance_address)

    @classmethod
    def push_financials(cls):
        return


if __name__ == '__main__':
    # AwsMessages.delete_index()
    fin = Financials()
    fin.fetch_financials()
