from datetime import datetime, timedelta, date, time
import pandas as pd
import time

from pytrends.request import TrendReq
from pytrends.exceptions import ResponseError
import pytrends.cache as cache

import sys
import functools



##################################################
# API
def get_daily_trend(trendreq, keyword:str, start:str, end:str, cat=0,
                    geo='', gprop='', delta=269, overlap=100, sleep=0, 
                    tz=0, verbose=False) ->pd.DataFrame:
    """Stich and scale consecutive daily trends data between start and end date.
    This function will first download piece-wise google trends data and then 
    scale each piece using the overlapped period. 
        Parameters
        ----------
        trendreq : TrendReq
            a pytrends TrendReq object
        keyword: str
            currently only support single keyword, without bracket
        start: str
            starting date in string format:YYYY-MM-DD (e.g.2017-02-19)
        end: str
            ending date in string format:YYYY-MM-DD (e.g.2017-02-19)
        cat, geo, gprop, sleep: 
            same as defined in pytrends
        delta: int
            The length(days) of each timeframe fragment for fetching google trends data, 
            need to be <269 in order to obtain daily data.
        overlap: int
            The length(days) of the overlap period used for scaling/normalization
        tz: int
            The timezone shift in minute relative to the UTC+0 (google trends default).
            For example, correcting for UTC+8 is 480, and UTC-6 is -360 
    """

    path = f'interest_over_time/c{cat}/{"world" if not geo else geo}/{"web" if not gprop else gprop}/tz{tz}/daily/{keyword}'

    def fetcher(tf): return _fetch_data(sleep, trendreq, [keyword], timeframe=tf, cat=cat, geo=geo, gprop=gprop)
    def set_tz(df): df.index += timedelta(minutes=tz)
    def clr_tz(df): df.index -= timedelta(minutes=tz)

    df, start,end = cache.lookup(path), datetime.strptime(start, '%Y-%m-%d'), datetime.strptime(end, '%Y-%m-%d')

    # if data not in the cache
    if (type(df) == type(None)) or (len(df) == 0):
        df = _combine_sequence(_get_sequence(fetcher, start, end, delta, overlap))
        set_tz(df)
        df = cache.insert(path, df)
        return df[pd.Timestamp(start):pd.Timestamp(end)]

    # Get left and right sides of what we cached. e.g. we have all of 2018 and now need 2016
    clr_tz(df)
    st = df.index[0].to_pydatetime()
    et = df.index[-1].to_pydatetime()
    get_data_before_cache_start = start < st
    get_data_after_cache_start = end > et
    st += timedelta(days=100)
    et -= timedelta(days=100)
    ls = _get_sequence(fetcher, start, st, delta, overlap) if get_data_before_cache_start else []
    rs = _get_sequence(fetcher, et, end, delta, overlap)   if get_data_after_cache_start else []

    # stich together the pieces
    dfs = ls + [df] + rs
    df = _combine_sequence(dfs)
    set_tz(df)
    return cache.insert(path, df)[pd.Timestamp(start):pd.Timestamp(end)]

    '''
    # Correct the timezone difference

    dfs = ls + [df] + rs
    '''

##################################################
# Original code to compare against
def _reference_get_daily_trend(trendreq, keyword:str, start:str, end:str, cat=0,
                    geo='', gprop='', delta=269, overlap=100, sleep=0, 
                    tz=0, verbose=False) ->pd.DataFrame:

    """Stich and scale consecutive daily trends data between start and end date.
    This function will first download piece-wise google trends data and then 
    scale each piece using the overlapped period. 
        Parameters
        ----------
        trendreq : TrendReq
            a pytrends TrendReq object
        keyword: str
            currently only support single keyword, without bracket
        start: str
            starting date in string format:YYYY-MM-DD (e.g.2017-02-19)
        end: str
            ending date in string format:YYYY-MM-DD (e.g.2017-02-19)
        cat, geo, gprop, sleep: 
            same as defined in pytrends
        delta: int
            The length(days) of each timeframe fragment for fetching google trends data, 
            need to be <269 in order to obtain daily data.
        overlap: int
            The length(days) of the overlap period used for scaling/normalization
        tz: int
            The timezone shift in minute relative to the UTC+0 (google trends default).
            For example, correcting for UTC+8 is 480, and UTC-6 is -360 
    """

    # df.iloc[0].name.to_pydatetime() > zz
    
    start_d = datetime.strptime(start, '%Y-%m-%d')
    init_end_d = end_d = datetime.strptime(end, '%Y-%m-%d')
    init_end_d.replace(hour=23, minute=59, second=59)    
    delta = timedelta(days=delta)
    overlap = timedelta(days=overlap)

    itr_d = end_d - delta
    overlap_start = None

    df = pd.DataFrame()
    ol = pd.DataFrame()
    
    while end_d > start_d:
        tf = itr_d.strftime('%Y-%m-%d')+' '+end_d.strftime('%Y-%m-%d')
        if verbose: print('Fetching \''+keyword+'\' for period:'+tf)
        temp = _fetch_data(sleep, trendreq, [keyword], timeframe=tf, cat=cat, geo=geo, gprop=gprop)
        temp.drop(columns=['isPartial'], inplace=True)
        temp.columns.values[0] = tf
        ol_temp = temp.copy()
        ol_temp.iloc[:,:] = None
        if overlap_start is not None:  # not first iteration
            if verbose: print('Normalize by overlapping period:'+overlap_start.strftime('%Y-%m-%d'), end_d.strftime('%Y-%m-%d'))
            #normalize using the maximum value of the overlapped period
            y1 = temp.loc[overlap_start:end_d].iloc[:,0].values.max()
            y2 = df.loc[overlap_start:end_d].iloc[:,-1].values.max()
            coef = 1.0*y2/y1
            temp = temp * coef
            ol_temp.loc[overlap_start:end_d, :] = 1 

        df = pd.concat([df,temp], axis=1)
        ol = pd.concat([ol, ol_temp], axis=1)
        # shift the timeframe for next iteration
        overlap_start = itr_d
        end_d -= (delta-overlap)
        itr_d -= (delta-overlap)
    
    df.sort_index(inplace=True)
    ol.sort_index(inplace=True)
    #The daily trend data is missing the most recent 3-days data, need to complete with hourly data
    if df.index.max() < init_end_d : 
        tf = 'now 7-d'
        hourly = _fetch_data(sleep, trendreq, [keyword], timeframe=tf, cat=cat, geo=geo, gprop=gprop)
        hourly.drop(columns=['isPartial'], inplace=True)
        
        #convert hourly data to daily data
        daily = hourly.groupby(hourly.index.date).sum()
        
        #check whether the first day data is complete (i.e. has 24 hours)
        daily['hours'] = hourly.groupby(hourly.index.date).count()
        if daily.iloc[0].loc['hours'] != 24: daily.drop(daily.index[0], inplace=True)
        daily.drop(columns='hours', inplace=True)
        
        daily.set_index(pd.DatetimeIndex(daily.index), inplace=True)
        daily.columns = [tf]
        
        ol_temp = daily.copy()
        ol_temp.iloc[:,:] = None
        # find the overlapping date
        intersect = df.index.intersection(daily.index)
        if verbose: print('Normalize by overlapping period:'+(intersect.min().strftime('%Y-%m-%d'))+' '+(intersect.max().strftime('%Y-%m-%d')))
        # scaling use the overlapped today-4 to today-7 data
        coef = 1.0*df.loc[intersect].iloc[:,0].max() / daily.loc[intersect].iloc[:,0].max()
        daily = daily*coef
        ol_temp.loc[intersect,:] = 1
        
        df = pd.concat([daily, df], axis=1)
        ol = pd.concat([ol_temp, ol], axis=1)

    # taking averages for overlapped period
    df = df.mean(axis=1)
    ol = ol.max(axis=1)
    # merge the two dataframe (trend data and overlap flag)
    df = pd.concat([df,ol], axis=1)
    df.columns = [keyword,'overlap']
    # Correct the timezone difference
    df.index = df.index + timedelta(minutes=tz)
    df = df[start_d:init_end_d]
    # re-normalized to the overall maximum value to have max =100
    df[keyword] = 100*df[keyword]/df[keyword].max()
    
    return df

##################################################
# Helper fns
def _fetch_data(sleep, trendreq, kw_list, timeframe='today 3-m', cat=0, geo='', gprop='') -> pd.DataFrame:
    
    """Download google trends data using pytrends TrendReq and retries in case of a ResponseError."""
    attempts, fetched = 0, False
    while not fetched:
        try:
            trendreq.build_payload(kw_list=kw_list, timeframe=timeframe, cat=cat, geo=geo, gprop=gprop)
        except ResponseError as err:
            print(err)
            print(f'Trying again in {60 + 5 * attempts} seconds.')
            sleep(60 + 5 * attempts)
            attempts += 1
            if attempts > 3:
                print('Failed after 3 attemps, abort fetching.')
                break
        else:
            fetched = True

    # dont give up til we get the data!
    for i in range(0,10):
        t = 2**i*sleep
        # in case of short query interval getting banned by server
        try:
            time.sleep(t)
            print(f'fetching {timeframe}')
            return trendreq.interest_over_time()
        except IndexError as err:
            return None
            print('IndexError! ...',err)
        except:
            print(f'{sys.exc_info()[0]}\ntimeout... trying again. waiting {t} seconds \n')
    raise 'timeout problem, could not get data'


def _combine_sequence(dfs):
    if len(dfs) == 1: return dfs[0]

    def fn(x,y):
        overlap_start = y.iloc[0].name
        overlap_end   = overlap_start+(x.iloc[-1].name-overlap_start)   # removing freq component here .. Timestamp('2020-02-23 00:00:00', freq='D')
        y1 = y.loc[overlap_start:overlap_end].iloc[:,0].max()
        y2 = x.loc[overlap_start:overlap_end].iloc[:,-1].max()
        coef = 1.0*y2/y1
        y = y * coef
        df = pd.concat([x,y], axis=1)
        return df # pd.concat([x,y], axis=1)

    df = functools.reduce(fn,dfs)

    df.sort_index(inplace=True)
    df = df.mean(axis=1)                           # taking averages for overlapped period
    df = 100.0*df/df.max()  # re-normalized to the overall maximum value to have max =100

    # Convert from series to DataFrame and name column the date range
    if type(df) == pd.core.series.Series:
        df = df.to_frame()
        df.columns = [df.iloc[0].name.to_pydatetime().strftime('%Y-%m-%d')+' '+df.iloc[-1].name.to_pydatetime().strftime('%Y-%m-%d')]

    return df

def _get_sequence(fetcher, start_d, end_d, delta=269, overlap=100) ->pd.DataFrame:
    dfs = []
    init_end_d = end_d
    init_end_d.replace(hour=23, minute=59, second=59)    
    delta = timedelta(days=delta)
    overlap = timedelta(days=overlap)
    itr_d = end_d - delta

    while end_d > start_d:
        tf = itr_d.strftime('%Y-%m-%d')+' '+end_d.strftime('%Y-%m-%d')
        # if verbose: print('Fetching \''+keyword+'\' for period:'+tf)
        df = fetcher(tf)
        if type(df) == type(None): 
            break
        df.drop(columns=['isPartial'], inplace=True)
        df.columns.values[0] = tf
        dfs.append(df)
        end_d -= (delta-overlap)
        itr_d -= (delta-overlap)

    dfs.reverse()
    return dfs



