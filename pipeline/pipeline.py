import requests
import os
import pandas as pd
import json
from datetime import datetime, timedelta
from time import time
from utils import connect_to_database
from math import ceil
import psycopg2
import re


def main(symbols, intervals, limit):
    start_time = time()
    # CONNECTION TO THE DATABASE
    conn = connect_to_database()
    for symbol in symbols:
        for interval in intervals:
            table_name = symbol + '_' + interval
            params = _set_download_parameters(conn, table_name, interval)
            df = _download_candlestick_data(symbol, interval, limit, params)
            df = _remove_duplicates(df)
            _load_data_into_database(conn, df, table_name)
    conn.close()
    elapsed_time = time() - start_time
    print("\nElapsed time: %0.2f seconds." % elapsed_time)


def _check_if_table_exists(conn, table_name):
    query = f"""
            SELECT * FROM information_schema.tables 
            WHERE table_name = '{table_name}'
            """
    cur = conn.cursor()
    cur.execute(query)

    # print(cur.fetchone())
    table_exists = bool(cur.rowcount)
    cur.close()

    if table_exists:
        print(f'Table {table_name} exists and will be read...')
    else:
        # print(f'Table {table_name} does not exist and will be created...')
        _create_table(conn, table_name)
        print(f'Table {table_name} has been created...')
    return table_exists


def _create_table(conn, table_name):
    query = f"""
            CREATE TABLE IF NOT EXISTS public."{table_name}"
            (
                open_time timestamp without time zone NOT NULL,
                open numeric,
                high numeric,
                low numeric,
                close numeric,
                volume numeric,
                close_time timestamp without time zone,
                quote_asset_volume numeric,
                number_of_trades integer,
                taker_buy_base_asset_volume numeric,
                taker_buy_quote_asset_volume numeric,
                ignore integer,
                CONSTRAINT "{table_name}_pkey" PRIMARY KEY (open_time)
            )

            TABLESPACE pg_default;

            ALTER TABLE IF EXISTS public."{table_name}"
                OWNER to postgres;
            """
    cur = conn.cursor()
    cur.execute(query)
    cur.close()
    conn.commit()


def _set_download_parameters(conn, table_name, interval):

    # Check if table already exists
    table_exists = _check_if_table_exists(conn, table_name)

    # If table exists -> read it, otherwise set an empty dataframe

    if table_exists:
        sql_query = f"""
            SELECT open_time 
            FROM public."{table_name}";
            """
        df = pd.io.sql.read_sql_query(sql_query, conn)
        df = df.sort_values(by='open_time', ascending=True)
    else:
        df = pd.DataFrame([])

    if not df.empty:
        start_time = df.iloc[-1]['open_time'].to_pydatetime()
    else:
        start_time = datetime(2017, 1, 1)

    interval_timedelta = _interval_to_timedelta(interval)
    end_time = datetime.utcnow() + interval_timedelta
    # print(f'end_tome---{end_time}')

    interval_timedelta = _interval_to_timedelta(interval)

    nbr_intervals = int((end_time - start_time) / interval_timedelta)
    # print(f' timedelta: {nbr_intervals}')

    params = {'start_time': start_time,
              'end_time': end_time,
              'nbr_intervals': nbr_intervals
              }
    return params


def _interval_to_timedelta(interval):
    # intervals = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M']
    digit = int(re.findall(r'\d+', interval)[0])
    unit = interval[-1]

    if unit == 'm':
        minutes = 1
    elif unit == 'h':
        minutes = 60
    elif unit == 'd':
        minutes = 60 * 24
    elif unit == 'w':
        minutes = 60 * 24 * 7
    elif unit == 'M':
        minutes = 60 * 24 * 31

    return timedelta(minutes=digit * minutes)


def _download_candlestick_data(symbol, interval, limit, params):

    print(f'\n-------------------------------------------------------------------------------------------------------')
    print(
        f"* Downloading {symbol} data for {interval} interval, from {params['start_time']} to {params['end_time']} (UTC) *")
    print(f'-------------------------------------------------------------------------------------------------------')

    if params['nbr_intervals'] < limit:
        print(' - Downloading data in one round!')
        body = {"symbol": symbol,
                "interval": interval,
                "limit": str(limit),
                "startTime": int((params['start_time']-datetime.utcfromtimestamp(0)).total_seconds() * 1000),
                "endTime": int((params['end_time']-datetime.utcfromtimestamp(0)).total_seconds() * 1000)}

        # print(body)
        # df = pd.DataFrame([])

        df = _request_candlestick_data(body)
    else:
        print(
            f" - Downloading in several rounds (max: {ceil(params['nbr_intervals']/limit)})!")
        doc_columns = ['Open time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close time',
                       'Quote asset volume', 'Number of trades', 'Taker buy base asset volume', 'Taker buy quote asset volume', 'Ignore']
        doc_columns = [x.lower().replace(' ', '_') for x in doc_columns]
        df = pd.DataFrame(columns=doc_columns)

        pagination = True
        initial_round = True
        last_end_time = None
        count = 1

        while pagination:
            print(f"Round number: {count}")
            try:
                if initial_round:
                    print('First round')
                    body = {"symbol": symbol,
                            "interval": interval,
                            "limit": str(limit),
                            "startTime": None,
                            "endTime": None}  # If startTime and endTime are not sent, the most recent klines are returned.
                    initial_round = False
                else:
                    body = {"symbol": symbol,
                            "interval": interval,
                            "limit": str(limit),
                            "startTime": None,
                            "endTime": end_time}

                df_i = _request_candlestick_data(body)

                # Concatenating previous dafaframe (df) with this new round df_i
                df = pd.concat([df, df_i])
                df = df.sort_values(
                    by='open_time', ascending=True)
                end_time = int((df.iloc[0]['open_time'].to_pydatetime(
                )-datetime.utcfromtimestamp(0)).total_seconds() * 1000)

                # print(f"endtime: {end_time}")
                # print(pd.to_datetime(end_time, unit='ms'))

                if last_end_time == end_time:
                    print('Finishing fetching')
                    break
                last_end_time = end_time
                count += 1

            except:
                pagination = False
                print('failed')

    return df


def _request_candlestick_data(body):
    """
    This function donwloads data from Binance's api Kline/Candlestick Data endpoint.
    """
    url = 'https://api.binance.com/api/v3/klines'
    headers = {'accept': 'application/json'}

    # Downloading data for a given period -> making sure the data is downladed (status_code == 200)
    status = True
    try_number = 1
    sleep_time = 0.001  # s
    while status:
        response = requests.get(url, headers=headers, params=body)
        if response.status_code != 200:
            if try_number <= 10:
                print(
                    f"Status code is {response.status_code} at try number {try_number}, entering sleep for {sleep_time} second(s)")
                try_number += 1
                time.sleep(sleep_time)
            else:
                print(
                    f"Oops!  Maximum number of tries was reached ({try_number}).  Run the code again...")
                break
        else:
            print(
                f" - Data downloaded for {body['symbol']} data for {body['interval']} interval")
            status = False
    response = requests.get(url, headers=headers, params=body)
    data = response.json()
    doc_columns = ['Open time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close time',
                   'Quote asset volume', 'Number of trades', 'Taker buy base asset volume', 'Taker buy quote asset volume', 'Ignore']
    doc_columns = [x.lower().replace(' ', '_') for x in doc_columns]
    df = pd.DataFrame(data, columns=doc_columns)
    df = _columns_dtypes(df)

    return df


def _columns_dtypes(df):
    # ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time',
    #    'quote_asset_volume', 'number_of_trades', 'taker_buy_base_asset_volume',
    #    'taker_buy_quote_asset_volume', 'ignore']

    # .dt.tz_localize('UTC')#.dt.tz_convert('America/Santiago')
    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
    df['open'] = pd.to_numeric(df['open'])
    df['high'] = pd.to_numeric(df['high'])
    df['low'] = pd.to_numeric(df['low'])
    df['close'] = pd.to_numeric(df['close'])
    df['volume'] = pd.to_numeric(df['volume'])
    # .dt.tz_localize('UTC')#.dt.tz_convert('America/Santiago')
    df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')
    df['quote_asset_volume'] = pd.to_numeric(df['quote_asset_volume'])
    df['number_of_trades'] = df['number_of_trades'].astype(int)
    df['taker_buy_base_asset_volume'] = pd.to_numeric(
        df['taker_buy_base_asset_volume'])
    df['taker_buy_quote_asset_volume'] = pd.to_numeric(
        df['taker_buy_quote_asset_volume'])
    df['ignore'] = df['ignore'].astype(int)
    return df


def _remove_duplicates(df):
    # Sorting values
    df = df.sort_values(by='open_time', ascending=True)
    # Removing duplicates
    df.drop_duplicates(inplace=True)
    # Reseting indexes
    df.reset_index(drop=True, inplace=True)
    return df


def single_insert(conn, insert_req):
    """ Execute a single INSERT request """
    cursor = conn.cursor()
    try:
        cursor.execute(insert_req)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()


def _load_data_into_database(conn, df, table_name):

    # Inserting each row
    for index, row in df.iterrows():
        query = """
        INSERT into public."%s"(open_time, open, high, low, close, volume, close_time,
        quote_asset_volume, number_of_trades, taker_buy_base_asset_volume,
        taker_buy_quote_asset_volume, ignore) values('%s', %s, %s, %s, %s, %s, '%s', %s, %s, %s, %s, %s)
        ON CONFLICT (open_time) DO UPDATE
        SET open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low, close = EXCLUDED.close, 
        volume = EXCLUDED.volume, close_time = EXCLUDED.close_time, quote_asset_volume = EXCLUDED.quote_asset_volume,
        number_of_trades = EXCLUDED.number_of_trades, taker_buy_base_asset_volume = EXCLUDED.taker_buy_base_asset_volume,
        taker_buy_quote_asset_volume = EXCLUDED.taker_buy_quote_asset_volume, ignore = EXCLUDED.ignore;
        """ % (table_name, row['open_time'].to_pydatetime(), row['open'], row['high'], row['low'],
               row['close'], row['volume'], row['close_time'].to_pydatetime(
        ), row['quote_asset_volume'],
            row['number_of_trades'], row['taker_buy_base_asset_volume'],
            row['taker_buy_quote_asset_volume'], row['ignore'])
        # print(query)
        single_insert(conn, query)


if __name__ == '__main__':
    symbols = ['BTCEUR',
               'ETHEUR',
               'BNBEUR']
    # symbols = ['AXSBUSD', 'MANABUSD', 'LUNABUSD']
    # intervals = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d', '3d', '1w', '1M']
    intervals = ['1h', '4h', '1d', '1w', '1M']
    limit = 1000
    start_time = None  # 1648965600000
    end_time = None  # 1648969200000
    main(symbols, intervals, limit)
