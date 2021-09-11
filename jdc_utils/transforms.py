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

def collapse_checkall(df, columns, checked='Checked',
                      multi_checked='Multiple checked',
                      none_checked='None checked', fillna=False, labels=None):
    """Collapse multiple check-all-that-apply fields into one"""
    
    bcols = df[columns]==checked
    var = bcols.idxmax(axis=1).where(bcols.sum(axis=1)==1, multi_checked).\
                               where(bcols.sum(axis=1)>0, none_checked)
    
    var = var.where((df[columns].notnull().sum(axis=1)==len(columns)) |
                    (var==multi_checked), None)
    
    if labels:
        var.replace(dict(zip(columns,labels)), inplace=True)
    
    if fillna:
        var.fillna(fillna, inplace=True)
    
    return var
