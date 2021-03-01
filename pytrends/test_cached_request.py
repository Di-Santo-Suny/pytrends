import numpy as np

from pytrends.request import TrendReq
from pytrends.exceptions import ResponseError
import pytrends.cache as cache
import pytrends.cached_request as c_req

print('Starting test cache request test')

preq =  TrendReq()
keyword = 'bitcoin'
tz = 360
path = 'interest_over_time/c0/world/web/tz{tz}/daily/{keyword}'

# Dont delete old data
restore = cache.lookup('bitcoin')
cache.delete('bitcoin')

# Test grabing data at various times gets the reference result
expected = c_req._reference_get_daily_trend(preq, 'bitcoin', start='2012-6-9', end='2020-10-4', sleep=3, tz=tz)
c_req.get_daily_trend(preq, 'bitcoin', start='2017-6-9', end='2017-10-4', sleep=3, tz=tz)
c_req.get_daily_trend(preq, 'bitcoin', start='2015-6-9', end='2018-10-4', sleep=3, tz=tz)
c_req.get_daily_trend(preq, 'bitcoin', start='2013-1-9', end='2013-10-4', sleep=3, tz=tz)
c_req.get_daily_trend(preq, 'bitcoin', start='2020-6-9', end='2020-10-4', sleep=3, tz=tz)
actual = c_req.get_daily_trend(preq, 'bitcoin', start='2012-6-9', end='2020-10-4', sleep=3, tz=tz)

# Dont delete old data
cache.insert('bitcoin', restore)

# Test
date_mistmatch   = (actual.index-actual.index).max().value
largest_val_err1 = (expected.bitcoin.values-np.transpose(actual.values)).min()
largest_val_err2 = (expected.bitcoin.values-np.transpose(actual.values)).max()

print('Numeric debug info:',date_mistmatch,largest_val_err1,largest_val_err2)

if date_mistmatch != 0:
    raise 'date mismatch'
if largest_val_err1 < -3.5:
    print('error: ',largest_val_err1)
    raise 'too large a mistmatch'
if largest_val_err2 > 3.5:
    print('error: ',largest_val_err2)
    raise 'too large a mistmatch'

print('Test passes!')
