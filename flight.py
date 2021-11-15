"""modules for queries and getting information"""
from dateutil.relativedelta import relativedelta
from multiprocessing.dummy import Pool
from itertools import product
from time import sleep
from requests.exceptions import ConnectionError
import requests
import datetime
import json
import pytz
import pandas as pd
import os
import tqdm

headers = {
  'X-Access-Token': '4103a017b9dc62d2b2e0a224a384281b'
}

def trying(url, data):
    resp = requests.get(url, params=data, headers=headers)
    if resp.status_code == 200:
        pass
    else:
        with open('errors.txt', 'a') as file:
            file.write('---' + str(resp.status_code) + '---')
    return resp


def resnonse_url(url, data):
    works = None
    while works is None:
        try:
            resp = trying(url, data)
            works = 1
            return resp
        except:
            with open('errors.txt', 'a') as file:
                file.write(' try-' + str(data))
            pass


def find_flight(origin, destination, departure_date):
    url = ' http://min-prices.aviasales.ru/calendar_preload'
    data = {
        'origin': origin,
        'destination': destination,
        'departure_date': departure_date,
        'one_way': True,
    }
    resp = resnonse_url(url, data)
    data1 = json.loads(resp.text)['best_prices']

    tickets = 0
    price, number_of_changes = 0, 0
    for i in range(len(data1)):
        if data1[i]['depart_date'] == departure_date:
            price = int(data1[i]['value'])
            number_of_changes = data1[i]['number_of_changes']
            tickets = 1
    return price, number_of_changes, tickets


def write_line(origin, destination, departure_date):
    MSC_time = pytz.timezone('Europe/Moscow')
    datetime_MSC = datetime.datetime.now(MSC_time)
    search_time = datetime_MSC.strftime("%H:%M:%S")
    search_date = datetime.date.today()
    price, number_of_changes, tickets = find_flight(origin, destination, departure_date)
    if tickets == 1:
        df = pd.DataFrame({'search_date': search_date, 'search_time': search_time, 'origin': origin,
            'destination': destination, 'departure_date': departure_date, 'price': price,
            'number_of_changes': number_of_changes}, index=[0])
        # search_date,search_time,origin,destination,departure_date,price,number_of_changes
        df.to_csv(str(search_date)[:7] + '.csv', sep=',', header=None, mode='a', index=False)
    else:
        with open('errors.txt', 'a') as file:
            file.write(f"{origin}, {destination}, {departure_date}")


def wrap(args):
    res = write_line(*args)
    return res


if __name__ == '__main__':
    origin_all = ['MOW', 'LED']
    destination_all = ['BUD', 'ROM', 'PSA', 'PAR', 'STO', 'MAD', 'LIS',
    'TIV', 'HAM', 'SKP', 'VCE', 'TLV', 'AYT', 'DXB', 'BKK', 'EVN', 'REK',
    'LON', 'DUB', 'BRU', 'MIL', 'MUC', 'AMS', 'BER', 'VIE', 'PRG', 'OSL',
    'WAW', 'BTS', 'BEG', 'VNO', 'BUH', 'SOF', 'RIX', 'TLL', 'HEL', 'TIA', 
    'CPH', 'BCN', 'GVA', 'ZRH', 'DBV', 'IST', 'TBS', 'ATH', 'LUX']
    search_date = datetime.date.today()
    three_mon_rel = relativedelta(months=4)
    end_date = search_date + three_mon_rel
    list_of_dates = pd.date_range(
        min(search_date, end_date),
        max(search_date, end_date)
        ).strftime('%Y-%m-%d').tolist()
    all_row = len(list_of_dates)*len(origin_all)*len(destination_all)

    # for date, city_from, city_to in product(list_of_dates, origin_all, destination_all):
    #     write_line(city_from, city_to, date)
    # with Pool(20) as pool:
    #     pool.map(wrap, product(origin_all, destination_all, list_of_dates))
    with Pool(16) as p:
        r = list(tqdm.tqdm(p.imap_unordered(wrap, product(origin_all, destination_all, list_of_dates)), total=all_row))

    print('well done')
