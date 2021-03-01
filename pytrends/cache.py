from pandas import HDFStore,DataFrame

def insert(path, df):
    hdf = HDFStore('storage.h5')
    hdf[path] = df
    hdf.close()
    del hdf
    return df

def lookup(path):
    hdf = HDFStore('storage.h5')
    df = hdf[path] if path in hdf else None
    hdf.close()
    del hdf
    return df

def delete(path):
    hdf = HDFStore('storage.h5')
    if path in hdf:
        hdf.remove(path)
    hdf.close()
    del hdf
