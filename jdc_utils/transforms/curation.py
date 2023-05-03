import re
from collections import OrderedDict
from pathlib import Path

import pandas as pd
import pandas_flavor as pf
import petl as etl


# Note: alternative to using pandas-flavor is making a child class of pd.DataFrame
# all registered functions should transforms df inplace or have capability of inplace
# if making an inplace option to registered functions, make inplace=True as default.
@pf.register_dataframe_method
def to_quarter(df, from_date_name_to_quarter_name, inplace=True):
    """
    adds a quarter column by converting a date-like column
    into a quarter
    """
    for from_date_name, to_quarter_name in from_date_name_to_quarter_name.items():
        var = pd.to_datetime(df[from_date_name])
        quarters = pd.PeriodIndex(var, freq="Q")
        if inplace:
            df[to_quarter_name] = quarters
        else:
            return quarters
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
    """convert column names to lower case"""
    df = df.copy()
    df.columns = [c.lower() for c in df.columns]
    return df


@pf.register_dataframe_method
def to_new_names(df, mappings, old_and_new_names=False):
    """
    Converts column names in the given DataFrame to new names
    based on predefined mappings (from jcoin core_measure frictionless schema)

    Parameters
    -----------
    df: pandas.DataFrame
        Input DataFrame whose column names need to be converted.

    mappings: dict
        A dictionary of old name: new name mappings used to rename columns
    old_and_new_names: bool, optional (default=False)
        If True, returns df with Multiindex columns where two levels
        are old and new names

    Returns
    --------
    pandas.DataFrame
        DataFrame with converted column names if old_and_new_names=False
    """
    mappings_for_df = {old: new for old, new in mappings.items() if old in df.columns}

    df_output = df.copy()
    if mappings_for_df:
        if old_and_new_names:
            multiindex = [(old, mappings_for_df.get(old, None)) for old in df.columns]
            df_output.columns = pd.MultiIndex(
                multiindex, names=["oldnames", "newnames"]
            )
        else:
            df_output.rename(columns=mappings_for_df, inplace=True)

    return df_output


@pf.register_dataframe_method
def add_missing_fields(df, field_list, missing_value="Missing"):
    tbl = etl.fromdataframe(df)
    fieldnames_in_data = tbl.fieldnames()
    fields_to_add = []
    for fieldname in field_list:
        if fieldname not in fieldnames_in_data:
            fields_to_add.append((fieldname, missing_value))

    targetdf = tbl.addfields(fields_to_add).cut(field_list).todf()
    return targetdf
