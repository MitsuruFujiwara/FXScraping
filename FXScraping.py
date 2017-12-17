import pandas as pd
import numpy as np
import sqlite3

class FXScraping(object):

    """
    A class for scraping and generate datasets of historical exhange rates.
    The rates are downloaded from Mizuho Bank's web site.
    https://www.mizuhobank.co.jp/rate/market/historical.html
    """

    def __init__(self):
        self.url_historical = 'https://www.mizuhobank.co.jp/rate/market/csv/quote.csv'
        self.url_current = 'https://www.mizuhobank.co.jp/rate/market/csv/tm_quote.csv'
        self.xxxusd = ['GBP', 'EUR', 'AUD', 'NZD']

    def __getData(self, url):
        data = pd.read_csv(url)
        col = list(data.ix[1, :])
        col[0] = 'date'
        col[32] = 'NA'
        data.columns = col
        data.index = pd.to_datetime(data.date)
        data = data.drop('NA', axis=1).drop('date', axis=1)
        data = data[2:]
        data = data.replace('*****', np.nan)
        return data

    def __getAgainstUSD(self, df):
        df = df.astype(float)
        _df = {}
        for c in df.columns:
            try:
                _df[c] = df[c] / df['USD'] if c in self.xxxusd else df['USD'] / df[c]
            except (ValueError, TypeError):
                continue
        _df['USD'] = list(df['USD'])
        return pd.DataFrame(_df, index=df.index)

    def getAllData(self):
        # generate datasets
        df_jpy = self.__getData(self.url_historical)
        df_usd = self.__getAgainstUSD(df_jpy)

        return df_jpy, df_usd

    def updateData(self):
        #TODO
        return None

    def saveData(self):
        # save csv
        df_jpy.to_csv('df_jpy.csv')
        df_usd.to_csv('df_usd.csv')

        # Save sql database #TODO
#        conn = sqlite3.connect("data.db")
#        df_jpy.to_sql('df_jpy', conn, if_exists='replace')
#        df_usd.to_sql('df_usd', conn, if_exists='replace')

        # Close DataBase
#        conn.close()

if __name__ == '__main__':
    fx = FXScraping()
    fx.getAllData()
