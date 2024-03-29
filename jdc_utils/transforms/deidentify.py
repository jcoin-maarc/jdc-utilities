"""
Functions to support
deidentification processes

Combines functions from dataforge to support
JDC specific needs.

"""
import re
from collections import OrderedDict
from functools import reduce
from pathlib import Path
import copy
import git
import numpy as np
import pandas as pd
import pandas_flavor as pf
from dataforge import ids, tools

versioned_filenames = {
    "shift_dates": "days_for_shift_date.csv",
    "replace_ids": "jdc_person_id.csv",
}


def _combine_mappings(mapfilepath):
    """
    join version controlled files into one
    for convenience and accessibility

    NOTE: this is an internal fxn for use with
    functions using versioned controlled files
    (ie must be in a directory with tmp/git/<version control repo>)
    """

    files = Path(".").glob("tmp/git/*/*.csv")
    dfs = [pd.read_csv(f) for f in files]

    id_column = list(
        reduce(
            lambda dfx, dfy: set(dfx.columns.tolist()).intersection(
                dfy.columns.tolist()
            ),
            dfs,
        )
    )
    merge = lambda dfx, dfy: dfx.merge(dfy, on=id_column, how="outer")
    return reduce(merge, dfs).to_csv(mapfilepath, index=False)


@pf.register_dataframe_method
def replace_ids(df, id_file, id_column, history_path):
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
    history_path = Path(history_path)
    id_map_file = versioned_filenames["replace_ids"]
    id_history_path = history_path.joinpath(id_map_file).with_suffix(".git")

    df_new = ids.replace_ids(
        df=df,
        id_file=id_file,
        map_file=id_map_file,
        column=id_column,
        map_url=id_history_path.as_posix(),
    )
    _combine_mappings(history_path.parent / (history_path.name + ".csv"))
    return df_new


@pf.register_dataframe_method
def shift_dates(df, id_column, date_columns, history_path, seed=None):
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
    history_path = Path(history_path)
    ids = df[id_column]
    offsets_map_file = versioned_filenames["shift_dates"]
    offsets_history_path = history_path.joinpath(offsets_map_file).with_suffix(".git")
    date_columns = copy.deepcopy(date_columns)
    offsets = tools.date_offset(
        key=ids,
        offset_file=offsets_map_file,
        name=offsets_history_path.stem,
        offset_url=offsets_history_path.as_posix(),
        seed=seed,
    )

    if isinstance(date_columns, str):
        date_columns = [date_columns]
    elif isinstance(date_columns,tuple):
        date_columns = list(date_columns)

    for col in date_columns:
        if not col in list(df):
            print(f"{col} not in dataframe so removing from the date column list")
            date_columns.remove(col)
        else:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # shift the dates with saved offsets
    df_new = tools.shift_dates(
        df=df, shift=offsets, date_cols=date_columns, merge_on=id_column
    )

    # change to format specified in schema
    for col in date_columns:
        df_new[col] = df_new[col].dt.strftime("%Y%m%d")

    # update mappings file
    _combine_mappings(history_path.parent / (history_path.name + ".csv"))
    return df_new


def deidentify(
    df,
    id_file,
    id_column,
    history_path,
    date_columns,
    fxns=["replace_ids", "shift_dates"],
):
    """wrapper function for all deidentification steps"""
    if "replace_ids" in fxns:
        df = replace_ids(
            df,
            id_file=id_file,
            id_column=id_column,
            history_path=history_path,
        )
    if "shift_dates" in fxns:
        if "replace_ids" in fxns:
            id_column = pd.read_csv(id_file).squeeze().name

        df = shift_dates(
            df,
            id_column=id_column,
            date_columns=date_columns,
            history_path=history_path,
        )
    return df


def init_version_history(file_history_path, overwrite=False):
    file_history_path = Path(file_history_path)

    if not Path(file_history_path).exists():
        print(str(file_history_path) + " does not exist so making directory")
        file_history_path.mkdir(exist_ok=True, parents=True)
        repo = git.Repo.init(file_history_path, bare=True)

    elif Path(file_history_path).exists() and overwrite:
        print(f"Overwriting {str(file_history_path)}")
        repo = git.Repo.init(file_history_path, bare=True)
    else:
        raise Exception("File history already exists and specified to not overwrite")

    return repo


def init_version_history_all(history_path, overwrite=False,**kwargs):
    history_path = Path(history_path)

    if not Path(history_path).exists():
        print(str(history_path) + " does not exist so making directory")
        history_path.mkdir(exist_ok=True, parents=True)

    for fxn, file_name in versioned_filenames.items():
        file_history_path = Path(history_path).joinpath(file_name).with_suffix(".git")
        _ = init_version_history(file_history_path, overwrite=overwrite)
