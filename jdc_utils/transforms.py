"""Data transformations"""

import pandas as pd
import yaml

def read_mapfile(mapfile):
    with open(mapfile) as file:
        return yaml.safe_load(file)

def map(df, mapfile):
    """Rename vars and/or replace values"""
    
    map = read_mapfile(mapfile)
    for var in map:
        df.rename(columns={var: map[var]['name']}, inplace=True)
        if 'values' in map[var] and map[var]['name'] in df:
            df[map[var]['name']].replace(map[var]['values'], inplace=True)

def to_quarter(datevar):
    
    var = pd.to_datetime(datevar)
    return pd.PeriodIndex(var, freq='Q')
