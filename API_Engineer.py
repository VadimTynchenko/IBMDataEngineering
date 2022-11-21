#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov 17 20:33:35 2022

@author: vadimtynchenko
"""
import requests
import pandas as pd


url = "https://api.apilayer.com/exchangerates_data/latest?base=EUR&apikey=qtPTIyJtY6uZaFLWMjuuRq1QntP1cgGu"


data = requests.get(url)

ex_df = pd.DataFrame(pd.read_csv("exchange_rates.csv", index_col=0))
exchange_rate = ex_df.loc['GBP', :]
print(exchange_rate)


def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process)
    return dataframe


columns = ['Name', 'Market Cap (US$ Billion)']


def extract():
    extracted_data = pd.DataFrame(columns=columns).append(
        extract_from_json("bank_market_cap_1.json"), ignore_index=True)
    return extracted_data


def transform(market, exchange_rate):
    market['Market Cap (GBP$ Billion)'] = market['Market Cap (US$ Billion)']
    market['Market Cap (GBP$ Billion)'] *= exchange_rate['Rates'].tolist()
    market.drop('Market Cap (US$ Billion)', axis=1, inplace=True)
    return market


def load(data):
    # Write your code here
    data.to_csv("bank_market_cap_gbp.csv", index=False)


def log(message):
    # Write your code here
    with open('log.txt', 'a') as f:
        f.write('{}\n'.format(message))


log('ETL Job Started')

log('Extract phase Started')

df = extract()

df.head()

log("Extract phase Ended")

log("Transform phase Started")

df = transform(df, exchange_rate)
# Print the first 5 rows here
df.head()

log('Transform phase Ended')

log("Load phase Started")

load(df)

log("Load phase Ended")
