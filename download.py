# -*- coding: utf-8 -*-
from dataapi_win36 import Client
import pandas as pd

class Download(Client):

    def __init__(self):
        super().__init__()
        self.init('30aed6246ea1565f4778ba7d431a81775803e7506b393ca017c840ae67477c95')


    def _make_uri(self, basic_uri: str, **kwargs) -> str:
        kw_str = ''
        for key, value in kwargs.items():
            kw_str += key + '=' + str(value) +'&'
        return basic_uri + kw_str




    def fetch(self, table: str, **kwargs) -> pd.DataFrame:
        '''
        :param kwargs: beginDate, endDate, ticker, secIC and tradeDate
        :return: pd.DataFrame
        '''
        if table == "RMExposure":
            uri = self._make_uri("/api/equity/getRMExposureDay.json?", **kwargs)
        else:
            raise ValueError("Invalid table name: " + table)
        code, result = self.getData(uri)
        if code != 200:
            raise ConnectionError("Failed to get data from datayes with code: " + str(code))
        else:
            response = eval(result.decode())
            if response["retMsg"] == "No Data Returned":
                return None
            else:
                return pd.DataFrame(response["data"])



if __name__ == "__main__":
    obj = Download()
    print(obj.fetch("RMExposure", beginDate=20190102, endDate=20190102))