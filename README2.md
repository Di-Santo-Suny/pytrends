# Cached Daily Trend
Stores google daily trends in a single HDF5 file.

## Example Scripts
Open a python terminal and run:
```python
# Get data
import pytrends
import pytrends.cached_request
preq =  pytrends.request.TrendReq()
df = pytrends.cached_request.get_daily_trend(preq,'gamestop', start='2017-8-1',end='2021-2-20',sleep=3)

# Plot data
import matplotlib.pyplot as plt 
plt.plot(df)
plt.show()

# Save all current data as a csv
import pytrends.cache 
df = pytrends.cache.lookup('interest_over_time/c0/world/web/tz0/daily/gamestop')
df.to_csv('gamestop.csv')
```

## Notes
1) It stops from 1 to 169 days short of getting the earliest data. A fix is to ask Google the earliest data it has, then adjust the code based on that.
2) Backing up the data helps in event of hdf5 corruption.
3) If changes are made to code, run the test file pytrends/test_cached_request.py. Do this by importing the file into the python shell.
