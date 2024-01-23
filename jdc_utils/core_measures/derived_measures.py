import pandas as pd
from jdc_utils.transforms import collapse_checkall
""" contains functions that derive variables from the submitted variables if not present in submission """

def combine_race(df):
    """
    Combines all race boolean (yes/no) variables into
    one racial category variable.
    """
    df = pd.DataFrame(df)
    # derived variables
    collapse_checkall_params = {
    "resulting_column_name": "race",
    "columns_to_labels": {
        "race_white": "White",
        "race_black": "Black or African American",
        "race_AIAN": "American Indian or Alaska Native",
        "race_asian": "Asian",
        "race_hawaiian_OPI": "Native Hawaiian or Other Pacific Islander",
        "race_other": "Some other race",
    },
        "multi_checked": "Multiracial",
        "none_checked": "Missing",
        "checked": "Yes",
        "fillna": "Missing",
    }

    df["race"] = collapse_checkall(df, **collapse_checkall_params,inplace=False)

    return df


def map_gender_id_condensed(df):
    """
    Maps the more detailed gender variable to 
    the shorter/condensed gender var
    if the field is missing (or all values in field are missing)
    """

    df = pd.DataFrame(df)
    gender_id_condensed = df["gender_id"].replace(
        {
            "Transgender man/trans man/female-to-male (FTM)":"Transgender",
            "Transgender woman/trans woman/male-to-female (MTF)":"Transgender",
            "Genderqueer/gender nonconforming/neither exclusively male nor female":"Gender nonconforming",
            "Additional gender category (or other)":"Something else"
        }
    )

    df.gender_id_condensed.where(lambda s:s!="Missing",gender_id_condensed,inplace=True)

    return df




