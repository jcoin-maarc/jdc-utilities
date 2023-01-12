import pandas as pd 
import re

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
        raise Exception("Data type not supported")
    return df