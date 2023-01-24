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

def combine_mappings(id_column,mapfilepath,tmp_historypath="tmp/git"):
    """ 
    join version controlled files into one
    for convenience and accessibility
    """
    files = Path(tmp_historypath).glob("/*/*.csv")
    merge = lambda dfx,dfy:dfx.merge(
        dfy,on=id_column,how='outer'
    )
    return reduce(merge,files).to_csv(mapfilepath,index=False)

@pf.register_dataframe_method
def replace_ids(df, id_file,map_file,history_path):
    """ 
    (pulls in the most up-to-date mappings stored in id_history_path).
    The id_history_path pulls in the most up to date id mappings. It is stored
    in a common/secure location such as a lab or institute server (e.g., P,J,etc drive)

    1. Pulls in the most up-to-date version of previously mapped ids (called id_history_path).
    2. Maps any new ids to local ids (ie id_column) and adds these to the mapping file.
    3. Stores these mappings in a temporary file in tmp/git/ids.csv) which is then added (ie pushed) to the 
    version control history in id_history_path and adds to mappings csv (not version controlled - just for convenience)

    Returns this new DataFrame

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
    combine_mappings(id_column, history_path.parent/(history_path.name+".csv"))
    return df_new



@pf.register_dataframe_method
def shift_dates(
        df,
        id_column,
        date_columns,
        history_path):
    """ 
    This wrapper function combines dataforge's offset and shift_dates function:
    1. Gets day offsets (one offset per individual
    specified by id from random number of days between -162 and  163);
    2. generates new offsets for new ids without day offsets, adds these
    to a file storing these id <> day offset mappings and pushes them to 
    a repository storing the history of these date shifts in a shared location
    for easier collaboration and higher security.
    3. Finally, all specified dates are shifted by these day offsets.
    Returns this new DataFrame and the mappings are also added to a csv 
    (not version controlled - just for convenience) beside the mapping history directory folder
    
    NOTES
    
    - offsets_history_path is a git bare repository, meaning this directory contains the entire history for mapped ids including
        the most recent version. 

    - See the [dataforge function documentation](https://gitlab.com/phs-rcg/data-forge/-/blob/main/src/dataforge/tools.py) for more general details
        on the shift_dates and date_offset functions used in this implementation.


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

    combine_mappings(id_column, history_path.parent/(history_path.name+".csv"))
    return df_new


def init_version_history(file_history_path,overwrite=False):

    file_history_path = Path(file_history_path)

    if not Path(file_history_path).exists():

        print(str(file_history_path)+" does not exist so making directory")
        file_history_path.mkdir(exist_ok=True,parents=True)
        repo = git.Repo.init(file_history_path,bare=True)

    elif Path(file_history_path).exists() and overwrite:
        print(f"Overwriting {str(file_history_path)}")
        repo = git.Repo.init(file_history_path,bare=True)
    else:
        raise Exception("File history already exists and specified to not overwrite")

    return repo

def init_version_history_all(history_path,overwrite=False):

    history_path = Path(history_path)

    if not Path(history_path).exists():
        print(str(history_path)+" does not exist so making directory")
        history_path.mkdir(exist_ok=True,parents=True)

    for fxn,file_name in versioned_filenames.items():
        file_history_path = (
            Path(history_path)
            .joinpath(file_name)
            .with_suffix(".git")
        )
        _ = init_version_history(file_history_path,overwrite=overwrite)
