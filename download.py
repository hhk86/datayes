# -*- coding: utf-8 -*-
from dataapi_win36 import Client
import pandas as pd
import pickle

class Download(Client):

    def __init__(self):
        super().__init__()
        self.init('30aed6246ea1565f4778ba7d431a81775803e7506b393ca017c840ae67477c95')


    def _make_uri(self, base_uri: str, **kwargs) -> str:
        kw_str = ''
        for key, value in kwargs.items():
            kw_str += key + '=' + str(value) +'&'
        return base_uri + kw_str


    def _table_map_baseuri(self, table: str):
        if table == "Exposure":
            return "/api/equity/getRMExposureDay.json?"
        elif table == "Factor":
            return "/api/equity/getRMFactorRetDay.json?"
        elif table == "Specific":
            return "/api/equity/getRMSpecificRetDay.json?"
        elif table == "CovarianceDay":
            return "/api/equity/getRMCovarianceDay.json?"
        elif table == "CovarianceShort":
            return "/api/equity/getRMCovarianceShort.json?"
        elif table == "CovarianceLong":
            return "/api/equity/getRMCovarianceLong.json?"
        elif table == "SriskDay":
            return "/api/equity/getRMSriskDay.json?"
        elif table == "SriskShort":
            return "/api/equity/getRMSriskShort.json?"
        elif table == "SriskLong":
            return "/api/equity/getRMSriskLong.json?"
        else:
            raise ValueError("Invalid table name: " + table)


    def fetch(self, table: str, **kwargs) -> pd.DataFrame:
        '''
        :param kwargs: beginDate, endDate, ticker, secIC and tradeDate
        :return: pd.DataFrame
        '''
        base_uri = self._table_map_baseuri(table)
        uri = self._make_uri(base_uri, **kwargs)
        code, result = self.getData(uri)
        if code != 200:
            raise ConnectionError("Failed to get data from datayes with code: " + str(code))
        else:
            response = eval(result.decode())
            if response["retMsg"] == "No Data Returned":
                return None
            else:
                return pd.DataFrame(response["data"])


    def download_data(self, start_date="20181101", end_date="20191225"):
        daily_data = dict()
        short_data = dict()
        long_data = dict()
        date_ls = pd.date_range(start_date, end_date)
        # print(date_ls)
        for t in date_ls:
            date = t.strftime("%Y%m%d")
            print(date)
            exposure = self.fetch("Exposure", beginDate=date, endDate=date)
            if exposure is None:
                continue
            temp_dict = dict()
            temp_dict["Exposure"] = exposure
            temp_dict["CovarianceDay"] = self.fetch("CovarianceDay", beginDate=date, endDate=date)
            temp_dict["SriskDay"] = self.fetch("SriskDay", beginDate=date, endDate=date)
            daily_data[date] = temp_dict
            temp_dict = dict()
            temp_dict["Exposure"] = exposure
            temp_dict["CovarianceShort"] = self.fetch("CovarianceShort", beginDate=date, endDate=date)
            temp_dict["SriskShort"] = self.fetch("SriskShort", beginDate=date, endDate=date)
            short_data[date] = temp_dict
            temp_dict = dict()
            temp_dict["Exposure"] = exposure
            temp_dict["CovarianceLong"] = self.fetch("CovarianceLong", beginDate=date, endDate=date)
            temp_dict["SriskLong"] = self.fetch("SriskLong", beginDate=date, endDate=date)
            long_data[date] = temp_dict

        with open("daily_data.pkl", "wb") as f:
            pickle.dump(daily_data, f)
        with open("short_data.pkl", "wb") as f:
            pickle.dump(short_data, f)
        with open("long_data.pkl", "wb") as f:
            pickle.dump(long_data, f)




if __name__ == "__main__":
    obj = Download()
    obj.download_data()