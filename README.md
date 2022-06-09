# Binance database

## ETL

The following sequence is performed for each required symbol-interval combination.

### Extract

* Connect to database "binance"
* Read table symbol_interval (i.e BTCEUR_1h)
    * If table does not exist -> Create a table called symbol_interval
* Set dowloading parameters:
    * start_time: 
        * Read last timestamp row and set start_time
        * if table does not exist -> Set as 01/01/2017
    * end_time: datetime.now()
    * nbr_intervals (number of time steps)
        * Calculate nbr_intervals = (end_time - start_time) / interval
* Download candlestick data from binance api.

### Transform
* Dates are converted from milliseconds to timestamps (%Y-%m-%d %H:%M:%S).

### Load
* Load data from pandas to postgres through library pyscopg2.