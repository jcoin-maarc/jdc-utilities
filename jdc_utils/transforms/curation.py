import pandas as pd

import re
from collections import OrderedDict
import pandas_flavor as pf
from pathlib import Path


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
def to_lowercase_names(df):
    """convert column names to lower case

    """
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]
    return df

