import pandas as pd
import numpy as np
import re
from collections import OrderedDict
import pandas_flavor as pf
from pathlib import Path
import git
from dataforge import tools,ids 


versioned_filenames = {
    'shift_dates':'days_for_shift_date.csv',
    'replace_ids':'jdc_person_id.csv'
}

def _get_versioned_history_path(fxnname,history_path):
    versioned_filename = Path(versioned_filenames[fxnname])
    file_history_path = (
        Path(history_path)
        .joinpath(versioned_filename)
        .with_suffix(".git")
    )
    return file_history_path

@pf.register_dataframe_method
def replace_ids(df, id_file, id_column,history_path):
    """ 
    (pulls in the most up-to-date mappings stored in id_history_path).
    The id_history_path pulls in the most up to date id mappings. It is stored
    in a common/secure location such as a lab or institute server (e.g., P,J,etc drive)

    1. Pulls in the most up-to-date version of previously mapped ids (called id_history_path).
    2. Maps any new ids to local ids (ie id_column) and adds these to the mapping file.
    3. Stores these mappings in a temporary file in tmp/git/ids.csv) which is then added (ie pushed) to the 
    version control history in id_history_path. 


    NOTES
    
    - id_history_path is a git bare repository, meaning this directory contains the entire history for mapped ids including
        the most recent version. 
    - The name of the new id column is determined by the column name in id_file.

    - See the [dataforge function documentation](https://gitlab.com/phs-rcg/data-forge/-/blob/main/src/dataforge/ids.py) for more general details
        on the replace id function used in this implementation.

    
    """
    id_history_path = _get_versioned_history_path('replace_ids',history_path)
    id_map_file = versioned_filenames['replace_ids']

    df_new = ids.replace_ids(
        df=df, 
        id_file=id_file,
        map_file=id_map_file,
        column=id_column,
        map_url=str(id_history_path)
    )

    return df_new



@pf.register_dataframe_method
def shift_dates(
        df,
        id_column,
        date_columns,
        history_path):
    """ 
    
    """

    ids = df[id_column]
    offsets_history_path = _get_versioned_history_path('replace_ids',history_path)
    offsets_map_file = versioned_filenames['replace_ids']

    offsets = tools.date_offset(
        key=ids,
        offset_file=offsets_map_file,
        name=offsets_history_path.stem,
        offset_url=str(offsets_history_path)
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