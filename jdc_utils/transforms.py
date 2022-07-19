"""Data transformations"""

import pandas as pd
import openpyxl
import xlrd
import yaml
import re
from collections import OrderedDict
import pandas_flavor as pf
from pathlib import Path
import jdc_utils.dataforge_tools as tools 
import jdc_utils.dataforge_ids as ids 


def read_df(file_path, **kwargs):
    """
    read in a data file based on
    the type of file

    """
    file_type = re.split("\.", str(file_path))[-1]

    if file_type == "csv":
        df = pd.read_csv(file_path, **kwargs)
    elif file_type == "tsv":
        df = pd.read_csv(file_path, sep="\t", **kwargs)
    elif file_type == "xlsx":
        df = pd.read_excel(file_path, engine="openpyxl", **kwargs)
    elif file_type == "xls":
        df = pd.read_excel(file_path, engine="xlrd", **kwargs)
    # TO ADD:
    # redcap, from internet
    else:
        sys.exit("Data type not supported")
    return df


def read_transformfile(transformfile):
    with open(transformfile) as file:
        return yaml.safe_load(file)


def run_transformfile(df, transformfile):
    """
    loop through transformations and mappings as specified
    in the yaml file

    then runs a given function name with a set of paramaters.
    Intended to transform the dataframe inplace.
    To provide compatability with native pandas fxns,
    the inplace argument assumed to be a parameter.

    If kwargs, then need to register a function
    calling the dictionary as keyword args in TransformDf class.
    """
    transform_mappings = OrderedDict(read_transformfile(transformfile))

    for fxn_name, params in transform_mappings.items():
        if fxn_name in ['replace_ids','shift_dates']:
            print(fxn_name)
            df = getattr(df, fxn_name)(**params)
            print(df.columns)
        else:
            getattr(df, fxn_name)(**params)
    
    return df 


# Note: alternative to using pandas-flavor is making a child class of pd.DataFrame
# all registered functions should transforms df inplace or have capability of inplace
# if making an inplace option to registered functions, make inplace=True as default.
@pf.register_dataframe_method
def to_quarter(df, from_date_name_to_quarter_name):
    """
    adds a quarter column by converting a date-like column
    into a quarter
    """
    for from_date_name, to_quarter_name in from_date_name_to_quarter_name.items():
        var = pd.to_datetime(df[from_date_name])
        quarters = pd.PeriodIndex(var, freq="Q")
        df[to_quarter_name] = quarters
    # return quarters


@pf.register_dataframe_method
def collapse_checkall(
    df,
    resulting_column_name,
    columns=None,
    columns_to_labels=None,
    checked="Checked",
    multi_checked="Multiple checked",
    none_checked="None checked",
    fillna=False,
    labels=None,
    inplace=True,
):
    """Collapse multiple check-all-that-apply fields into one"""

    # added option to make a columns to labels dict to make easier to see mappings
    if columns_to_labels and not columns and not labels:
        columns = columns_to_labels.keys()
        labels = columns_to_labels.values()

    if df.columns.isin(columns).sum() < len(columns):
        return None

    bcols = df[columns] == checked
    var = (
        bcols.idxmax(axis=1)
        .where(bcols.sum(axis=1) == 1, multi_checked)
        .where(bcols.sum(axis=1) > 0, none_checked)
    )

    var = var.where(
        (df[columns].notnull().sum(axis=1) == len(columns)) | (var == multi_checked),
        None,
    )

    if labels:
        var.replace(dict(zip(columns, labels)), inplace=True)

    if fillna:
        var.fillna(fillna, inplace=True)

    if inplace:
        df[resulting_column_name] = var
    else:
        return var


@pf.register_dataframe_method
def replace_all_values(df, from_value_to_value, inplace=True):
    df.replace(from_value_to_value, inplace=inplace)


@pf.register_dataframe_method
def add_columns_of_constants(df, column_name_and_value):
    for name, value in column_name_and_value.items():
        df[name] = value


@pf.register_dataframe_method
def rename_columns(df, from_name_to_name, inplace=True):
    df.rename(columns=from_name_to_name, inplace=inplace)


@pf.register_dataframe_method
def replace_column_values(df, within_column_from_value_to_value, inplace=True):
    df.replace(within_column_from_value_to_value, inplace=inplace)


@pf.register_dataframe_method
def combine_columns(df, new_name, cols):
    df[new_name] = ""
    for c in cols:
        df[new_name] += df[c].astype(str)


@pf.register_dataframe_method
def rename_and_change_values(df: pd.DataFrame, name_and_values: dict):
    """Rename vars and/or replace values"""
    for current_name, new_name_and_values in name_and_values.items():
        # added fillna as it could mean something different for each variable
        if current_name in df.columns:
            if "fillna" in new_name_and_values.keys():
                df[current_name].fillna(new_name_and_values["fillna"], inplace=True)
            if "values" in new_name_and_values.keys():
                df[current_name].replace(new_name_and_values["values"], inplace=True)
            if "name" in new_name_and_values.keys():
                df.rename(
                    columns={current_name: new_name_and_values["name"]}, inplace=True
                )


@pf.register_dataframe_method
def compute_days_btw_date_and_index(
    df: pd.DataFrame,
    index_date_col: str,
    index_date_type: str,
    date_to_days_names: dict,
    id_col: str = None,
    file_with_index: str = None,
    is_drop_dates: bool = True,
    inplace: bool = True,
):
    """
    creates new columns with days between a set of dates and an index date

    :param df: input dataframe that contains the dates to calculate against index date
    :param index_date_col: name of the index date column (either in the input dataframe or another dataframe)
    :param date_to_days_name: a dictionary consisting of the name of computed days between (key) and name of the date column in input dataset (value)
    :param file_with_index: the file path containing the dateframe of the index date

    TODO: Per Phil's request: randomize all dates by a factor and store this factor in id_mappings
    TODO: this function can generalize to other units pretty easily
    TODO: add params to merge on left and right as separate names
        (in this workflow it doesnt matter as same name for all ids is guaranteed by replace id fxn)
    """
    # index and dates both in input dataframe
    if not file_with_index:
        date_cols = list(date_to_days_names.values())
        date_and_index_df = df[[index_date_col] + date_cols].rename(
            columns={index_date_col: "index_date"}
        )
    # index in different dataframe
    else:
        # in addition to required index_date_col and df:
        assert (
            file_with_index
        ), "Need to specify file name if index date not in dataframe"
        assert id_col, "Need to specify an id_col to ensure proper merging"
        index_df = read_df(file_with_index)[[index_date_col, id_col]].rename(
            columns={index_date_col: "index_date"}
        )
        date_and_index_df = df.merge(
            index_df, on=id_col, how="left", validate="many_to_one"
        )

    # now calculate the merged series and add to input df
    index_date = pd.to_datetime(date_and_index_df["index_date"])
    for days_name, date_name in date_to_days_names.items():
        date = pd.to_datetime(date_and_index_df[date_name])
        df[days_name] = (date - index_date).dt.days

    df["index_date_type"] = index_date_type

    # other possible params specified
    if is_drop_dates:
        df.drop(columns=list(date_to_days_names.values()))
    if not inplace:
        return df


@pf.register_dataframe_method
def replace_ids(df, id_file, map_file, map_url=None, id_column=None):
    df_new = ids.replace_ids(df, id_file, map_file, map_url=None, column=id_column)
    return df_new


@pf.register_dataframe_method
def shift_dates(
        df,
        id_column,
        date_columns,
        map_file,
        map_url=None,
        shift_amount=365,
        keep_inputs=False,
):
    df_new = tools.shift_dates(
        df,
        date_cols=date_columns,
        map_file=map_file,
        id_col=id_column,
        map_url=map_url,
        shift_amount=shift_amount,
        keep_inputs=keep_inputs,
    )
    
    return df_new
