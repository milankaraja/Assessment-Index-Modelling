# By Vineeth Hari Naik, for Solactive
# Import data
import pandas as pd
from pandas import DataFrame, read_csv
import numpy as np
import matplotlib.pyplot as plt

import datetime
import datetime as dt
# BDay is business day, not birthday...
from pandas.tseries.offsets import BDay


class IndexModel:
    def __init__(self) -> None:
        # To be implemented
        pass
    def load_process_data(self,):
        file_stock_prices = 'C:\\Users\\vinee\\OneDrive\\Documents\\Assessment-Index-Modelling-master\\data_sources\\stock_prices.csv'
        stock_prices = pd.read_csv(file_stock_prices, parse_dates=['Date'])
        stock_prices["Date"]= pd.to_datetime(stock_prices ["Date"], errors='coerce')
        stock_prices = stock_prices.sort_index()
        #stock_prices["Date"]= pd.to_datetime(stock_prices ["Date"],format='%Y-%m-%d', errors='coerce')
        stock_prices.dropna()
        stock_prices["Date_forgroup"]=stock_prices["Date"]
        stock_prices.set_index('Date', inplace=True)
        return stock_prices
    def calc_index_level(self, start_date: dt.date, end_date: dt.date) -> None:
        # To be implemented
        y = self.load_process_data()
        stockp_firstbd = y.groupby(pd.Grouper(freq='M')).first()
        stockp_lastbd = y.groupby(pd.Grouper(freq='M')).last()
        stockp_firstbd.set_index('Date_forgroup', inplace=True)
        stockp_lastbd.set_index('Date_forgroup', inplace=True)
        RankValuesObj = stockp_lastbd.rank(axis=1)
        def get_col_name(row):
            b10 = (RankValuesObj.loc[row.name] == 10)
            b9 = (RankValuesObj.loc[row.name] == 9)
            b8 = (RankValuesObj.loc[row.name] == 8)
            return b10.index[b10.argmax()],b9.index[b9.argmax()],b8.index[b8.argmax()]
        Top3StocksLBD = RankValuesObj.apply(get_col_name, axis=1)
        def get_Top3_names(d):
            for i in range(0,len(Top3StocksLBD)):
                if stockp_lastbd.index[i] < d <= stockp_lastbd.index[i+1]:
                    return Top3StocksLBD[i]
        def get_Top3_prices(d):
            if d > stockp_lastbd.index[0]: 
                x = get_Top3_names(d)
                return y.loc[d,[x[0],x[1],x[2]]]
            else:
                return np.array([100,100,100])
        def quantity(d):
            if d <= stockp_lastbd.index[0]:
                return np.array([0.5,0.25,0.25])
            #elif: d= 
            else:
                for i in range(0,len(Top3StocksLBD)):
                    if stockp_lastbd.index[i] < d <= stockp_lastbd.index[i+1]:
                        return np.array(Top3Index(stockp_lastbd.index[i])*np.array([0.5,0.25,0.25])/get_Top3_prices(stockp_lastbd.index[i]))
        def Top3Index(d):
            
            if d <= stockp_lastbd.index[0]:
                return 100
            else:
                prices= get_Top3_prices(d)
                for i in range(0,len(Top3StocksLBD)):
                    if stockp_lastbd.index[i] < d <= stockp_lastbd.index[i+1]:
                        return sum(x * y for x, y in zip(quantity(stockp_firstbd.index[i]) ,prices ))
        
        

        y["Index"] = y["Date_forgroup"].apply(Top3Index)
        y["Weights"]= y["Date_forgroup"].apply(quantity)
                
                
        return y["Index"]
                                   

    def export_values(self, file_name: str) -> None:

        # To be implemented
        self.calc_index_level(dt.date, dt.date).to_csv (file_name, index = False, header=True)
        pass 

