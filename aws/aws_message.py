import json, requests
import uuid
from elasticsearch import Elasticsearch

from util.config import Config

end_point = Config.getAwsConfig()


class AwsMessages:
    twitter_mapping = "tweet"
    financials_mapping = "finance"
    stocks_mapping = "stock"

    twitter_index = "twitter/%s" % twitter_mapping
    stock_index = "stocks/%s" % stocks_mapping
    financials_index = "financials/%s" % financials_mapping

    twitter_address = '%s/%s/' % (end_point, twitter_index)
    stock_address = '%s/%s/' % (end_point, stock_index)
    finance_address = '%s/%s/' % (end_point, financials_index)

    @classmethod
    def upload_msg(cls, msg, index, mapping):
        print('saving tweets...')
        data = ''
        data += '{"index": {"_id": "%s"}}\n' % uuid.uuid4().hex
        data += json.dumps(msg) + '\n'
        address = '%s/%s/%s' % (end_point, index, mapping)
        # Upload tweets to elasticsearch
        print('uploading to databse...')
        upload_address = '%s/_bulk' % address
        response = requests.put(upload_address, data=data)
        print('upload success')
        print('elasticsearch response: %s' % response)

    @classmethod
    def create_index(cls):
        data = {
            "settings": {
                "number_of_shards": 2,
                "number_of_replicas": 1
            },
            "mappings": {
                cls.twitter_mapping: {
                    "properties": {
                        "name": {"type": "text"},
                        "time": {"type": "date", "format": "yyyy/MM/dd HH:mm:ss"},
                        "location": {"type": "geo_point"},
                        "text": {"type": "text"},
                        "profile_image_url": {"type": "text"},
                        "sentiment": {"type": "text"},
                        "message": {"type": "text"}
                    }
                }
            }
        }
        print(data)

        response = requests.post(cls.twitter_address, data=json.dumps(data),
                                 headers={"content-type": "application/json"})
        return response.text

    @classmethod
    def stock_index(cls):
        data = {
            "settings": {
                "number_of_shards": 2,
                "number_of_replicas": 1
            },
            "mappings": {
                cls.stocks_mapping: {
                    "properties": {
                        "adhocMargin": {"type": "text"},
                        "applicableMargin": {"type": "text"},
                        "averagePrice": {"type": "text"},
                        "basePrice": {"type": "text"},
                        "bcEndDate": {"type": "text"},
                        "bcStartDate": {"type": "text"},
                        "buyPrice1": {"type": "text"},
                        "buyPrice2": {"type": "text"},
                        "buyPrice3": {"type": "text"},
                        "buyPrice4": {"type": "text"},
                        "buyPrice5": {"type": "text"},
                        "buyQuantity1": {"type": "text"},
                        "buyQuantity2": {"type": "text"},
                        "buyQuantity3": {"type": "text"},
                        "buyQuantity4": {"type": "text"},
                        "buyQuantity5": {"type": "text"},
                        "change": {"type": "text"},
                        "closePrice": {"type": "text"},
                        "cm_adj_high_dt": {"type": "text"},
                        "cm_adj_low_dt": {"type": "text"},
                        "cm_ffm": {"type": "text"},
                        "companyName": {"type": "text"},
                        "css_status_desc": {"type": "text"},
                        "dayHigh": {"type": "text"},
                        "dayLow": {"type": "text"},
                        "deliveryQuantity": {"type": "text"},
                        "deliveryToTradedQuantity": {"type": "text"},
                        "exDate": {"type": "text"},
                        "extremeLossMargin": {"type": "text"},
                        "faceValue": {"type": "text"},
                        "high52": {"type": "text"},
                        "indexVar": {"type": "text"},
                        "isExDateFlag": {"type": "text"},
                        "isinCode": {"type": "text"},
                        "lastPrice": {"type": "text"},
                        "low52": {"type": "text"},
                        "marketType": {"type": "text"},
                        "ndEndDate": {"type": "text"},
                        "ndStartDate": {"type": "text"},
                        "open": {"type": "text"},
                        "pChange": {"type": "text"},
                        "previousClose": {"type": "text"},
                        "priceBand": {"type": "text"},
                        "pricebandlower": {"type": "text"},
                        "pricebandupper": {"type": "text"},
                        "purpose": {"type": "text"},
                        "quantityTraded": {"type": "text"},
                        "recordDate": {"type": "text"},
                        "secDate": {"type": "text"},
                        "securityVar": {"type": "text"},
                        "sellPrice1": {"type": "text"},
                        "sellPrice2": {"type": "text"},
                        "sellPrice3": {"type": "text"},
                        "sellPrice4": {"type": "text"},
                        "sellPrice5": {"type": "text"},
                        "sellQuantity1": {"type": "text"},
                        "sellQuantity2": {"type": "text"},
                        "sellQuantity3": {"type": "text"},
                        "sellQuantity4": {"type": "text"},
                        "sellQuantity5": {"type": "text"},
                        "series": {"type": "text"},
                        "surv_indicator": {"type": "text"},
                        "symbol": {"type": "text"},
                        "totalBuyQuantity": {"type": "text"},
                        "totalSellQuantity": {"type": "text"},
                        "totalTradedValue": {"type": "text"},
                        "totalTradedVolume": {"type": "text"},
                        "varMargin": {"type": "text"}
                    }
                }
            }
        }
        print(data)

        response = requests.post(cls.stock_address, data=json.dumps(data), headers={"content-type": "application/json"})
        print(response.text)
        return response.text

    @classmethod
    def financials_index(cls):
        data = {
            "settings": {
                "number_of_shards": 2,
                "number_of_replicas": 1
            },
            "mappings": {
                cls.financials_mapping: {
                    "properties": {
                        "Information": {"type": "text"},
                        "Symbol": {"type": "text"},
                        "Last Refreshed": {"type": "text"},
                        "Interval": {"type": "text"},
                        "Output Size": {"type": "text"},
                        "Time Zone": {"type": "text"},
                        "Time Series (1min)": {"type": "text"}

                    }
                }
            }
        }
        print(data)

        response = requests.post(cls.finance_address, data=json.dumps(data),
                                 headers={"content-type": "application/json"})
        print(response.text)
        return response.text

    @classmethod
    def delete_index(cls):
        client = Elasticsearch(
            ['https://search-ticker-sentiment-ohr4wryq6vcybcoqvqumx5bezm.us-east-2.es.amazonaws.com'])
        print(client.info())
        client.indices.delete(index='tweet', ignore=[400, 404])
        client.indices.delete(index='twitter', ignore=[400, 404])
        client.indices.delete(index='financials', ignore=[400, 404])
        client.indices.delete(index='stocks', ignore=[400, 404])
        print(client.info())

    @classmethod
    def download_msg(cls,index,mapping):
        data = {
            "size": 2000,

            "query": {
                "query_string": {"query": "msft"}
            },
            "sort": {"LastTradeDateTime": {"order": "desc"}}

        }
        search_address = '%s/%s/%s/_search' % (end_point,index,mapping)
        headers = {'Content-type': 'application/json', 'charset': 'UTF-8'}
        response = requests.post(search_address, data=json.dumps(data), headers=headers)
        result = response.json()
        print("Response from search ", result)
        return


if __name__ == '__main__':
    # AwsMessages.delete_index()
    AwsMessages.create_index()
    AwsMessages.financials_index()
    AwsMessages.stock_index()