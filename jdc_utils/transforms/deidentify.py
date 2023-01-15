import pandas as pd
import numpy as np
import re
from collections import OrderedDict
import pandas_flavor as pf
from pathlib import Path
from dataforge import tools,ids 

@pf.register_dataframe_method
def replace_ids(df, id_file, map_file, map_url=None, id_column=None):
    df_new = ids.replace_ids(
        df=df, 
        id_file=id_file, 
        map_file=map_file,
        map_url=None, 
        column=id_column)
    return df_new


@pf.register_dataframe_method
def shift_dates(
        df,
        id_column,
        date_columns,
        map_file,
        map_url=None,
        shift_days_col='days_for_shift_date'
):
    #create/save offsets (ie shift amounts)
    #TODO: add/test offset url and ensure map file works
    ids = df[id_column]

    if Path(map_file).exists():
        map_df = pd.read_csv(map_file)
        if shift_days_col in map_df:
            offsets = map_df.set_index(id_column)[shift_days_col]
            offsets.to_csv(shift_days_col+'.csv')
        else:
            offsets = None

    offsets = tools.date_offset(
        key=ids,
        offset_file=shift_days_col+'.csv',
        name=shift_days_col,
        offset_url=map_url
    )

    date_columns = [date_columns] if isinstance(date_columns,str) else date_columns

    for col in date_columns:
        df[col] = pd.to_datetime(df[col])

    #shift the dates with saved offsets
    df_new = tools.shift_dates(
        df=df,
        shift=offsets,
        date_cols=date_columns,
        merge_on=id_column
    )

    #change to format specified in schema
    for col in date_columns:
        df_new['shifted_' + col] = df_new[col].dt.strftime('%Y%m%d')
        del df_new[col]
    
    return df_new