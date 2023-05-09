"""
data commons transforms needed to map to sheepdog data model from validated frictionless datasets
"""

import pandas as pd
from frictionless import Resource

from .curation import collapse_checkall

# NOTE: these are based on the logical representations rather than
# the physical representations. That is, for example,
# while booleanFalse values are physically represented as "Yes",
# they are converted to the logical rep in the .to_pandas() method.
race_map = {
    "resulting_column_name": "race",
    "columns_to_labels": {
        "race_white": "White",
        "race_black": "Black or African American",
        "race_AIAN": "American Indian or Alaska Native",
        "race_asian": "Asian",
        "race_hawaiian_OPI": "Native Hawaiian or Other Pacific Islander",
        "race_other": "Some other race",
    },
    "multi_checked": "Some other race",
    "none_checked": "Not reported",
    "checked": True,
    "fillna": "Not reported",
    "inplace": False,
}

recode_map = {
    "visit_type": {"Baseline": "Baseline Visit", "Follow-up": "Follow-up Visit"},
    "hispanic_latino": {True: "Yes", False: "No"},
    "gender_id": {
        "Transgender man/trans man/female-to-male (FTM)": "Transgender",
        "Transgender woman/trans woman/male-to-female (MTF)": "Transgender",
        "Genderqueer/gender nonconforming/neither exclusively male nor female": "Gender nonconforming",
        "Additional gender category (or other)": "Something else",
    },
    "current_study_status": {"Unknown": "On study"},
}


def _choose_gender_field(df):
    # NOTE: all the physical representations of missing values
    # eg., Missing, Legitiamately skipped etc have been converted
    # to NA via the frictionless's Resource method .to_pandas
    select_condensed_col = (
        df["gender_id"].isna().sum() > df["gender_id_condensed"].isna().sum()
    )

    if select_condensed_col:
        return df["gender_id_condensed"]
    else:
        return df["gender_id"].replace(recode_map["gender_id"])


def to_participant_node(baseline_df):
    """
    Takes the validated core measure baseline
    dataset from core measure pacakge and return
    a dataframe for participant node of JDC sheepdog data model

    NOTE: role_in_project not in fricitonless core_measure_schema (added for explorer viz purposes)
    """

    node_df = pd.DataFrame(
        {
            "protocols.submitter_id": "main",
            "submitter_id": baseline_df.index.get_level_values("jdc_person_id"),
            "role_in_project": baseline_df["role_in_project"],
            "quarter_recruited": baseline_df["quarter_enrolled"],
            "current_client_status": baseline_df["current_study_status"].replace(
                recode_map["current_study_status"]
            ),
        }
    )
    return node_df.set_index("submitter_id")


def to_demographic_baseline_node(baseline_df):
    submitter_id = baseline_df.index.get_level_values("jdc_person_id")
    node_df = pd.DataFrame(
        {
            "participants.submitter_id": submitter_id,
            "submitter_id": submitter_id,
            "gender": _choose_gender_field(baseline_df),
            "hispanic": (
                baseline_df["hispanic_latino"]
                .replace(recode_map["hispanic_latino"])
                .fillna("Not reported")
            ),
            "race": collapse_checkall(baseline_df, **race_map),
        }
    )
    return node_df.set_index("submitter_id")


def to_enrollment_node(baseline_df):
    submitter_id = baseline_df.index.get_level_values("jdc_person_id")
    node_df = pd.DataFrame(
        {"participants.submitter_id": submitter_id, "submitter_id": submitter_id}
    )
    return node_df.set_index("submitter_id")


def to_time_point_node(time_point_df):
    get_lvl = time_point_df.index.get_level_values
    submitter_id = get_lvl("jdc_person_id") + "v" + get_lvl("visit_number").astype(str)
    node_df = pd.DataFrame(
        {
            "participants.submitter_id": get_lvl("jdc_person_id"),
            "submitter_id": submitter_id,
            "visit_type": time_point_df["visit_type"].replace(recode_map["visit_type"]),
        }
    )
    return node_df.set_index("submitter_id")


def _to_baseline_nodes(baseline_df, role):
    participant = to_participant_node(baseline_df).assign(role_in_project=role)
    resources = [
        {"name": "participant", "data": participant},
        {"name": "enrollment", "data": to_enrollment_node(baseline_df)},
        {
            "name": "demographic_baseline",
            "data": to_demographic_baseline_node(baseline_df),
        },
    ]
    return [Resource(**resource, format="pandas") for resource in resources]


def to_baseline_nodes(baseline_df):
    return _to_baseline_nodes(baseline_df, "Client")


def to_staff_baseline_nodes(baseline_df):
    return _to_baseline_nodes(baseline_df, "Staff")


def to_time_point_nodes(timepoints_df):
    resources = [{"name": "time_point", "data": to_time_point_node(timepoints_df)}]
    return [Resource(**resource, format="pandas") for resource in resources]
