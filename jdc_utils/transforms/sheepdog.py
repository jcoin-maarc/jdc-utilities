""" 
data commons transforms needed to map to sheepdog data model 
which are common across hubs/datasets
""" 

from .curation import collapse_checkall
import pandas as pd
   
race_map = {
    "resulting_column_name": "race",
    "columns_to_labels": {'d3_white': 'White',
        'd3_black': 'Black or African American',
        'd3_american_indian': 'American Indian or Alaska Native',
        'd3_asian': 'Asian',
        'd3_hawaiian': 'Native Hawaiian or Other Pacific Islander',
        'd3_other': 'Some other race'},
    "multi_checked": 'Some other race',
    "none_checked": 'Not reported',
    "checked": 'Yes',
    "fillna": 'Not reported',
    "inplace":False
}

recode_map = {
    "visit_type":
        {"Baseline": "Baseline Visit",
        "Follow-up": "Follow-up Visit"},
    "gender_id":{
        "Transgender man/trans man/female-to-male (FTM)":"Transgender",
        "Transgender woman/trans woman/male-to-female (MTF)":"Transgender",
        "Genderqueer/gender nonconforming/neither exclusively male nor female":"Gender nonconforming",
        "Additional gender category (or other)":"Something else",
        "Missing":"Not reported",
        "Legitimately skipped":"Not reported"
    }
}

def _choose_gender_field(df):
    # NOTE: all the physical representations of missing values
     # eg., Missing, Legitiamately skipped etc have been converted
     # to NA via the frictionless's Resource method .to_pandas
    select_gender_col = (
        df["gender_id"].isna().sum() < df["gender_id_condensed"].isna().sum()
    )

    if select_gender_col:
        return df["gender_id_condensed"]
    else:
        return (
            df["gender_id"]
            .replace(recode_map["gender_id"])
        )

def to_participant_node(baseline_df):
    """
    Takes the validated core measure baseline 
    dataset from core measure pacakge and return 
    a dataframe for participant node of JDC sheepdog data model

    NOTE: role_in_project not in fricitonless core_measure_schema (added for explorer viz purposes)
    """ 



    node_df = pd.DataFrame({
        "protocols.submitter_id":"main",
        "submitter_id":baseline_df.index.get_level_values("jdc_person_id"),
        "role_in_project":baseline_df["role_in_project"],
        "quarter_recruited":baseline_df["quarter_enrolled"],
        "current_client_status":baseline_df["current_study_status"]
    })
    return node_df

def to_demographic_baseline_node(baseline_df):
    submitter_id = baseline_df.index.get_level_values("jdc_person_id")
    node_df = pd.DataFrame({
        "participants.submitter_id":submitter_id,
        "submitter_id":submitter_id,
        "gender":_choose_gender_field(baseline_df),
        "hispanic":baseline_df["hispanic_latino"],
        "race":collapse_checkall(baseline_df,**race_map),
    })
    return node_df

def to_enrollment_node(baseline_df):
    submitter_id = baseline_df.index.get_level_values("jdc_person_id")
    node_df = pd.DataFrame({
        "participants.submitter_id":submitter_id,
        "submitter_id":submitter_id
    })
    return node_df

def to_time_point_node(time_point_df):
    get_lvl = time_point_df.index.get_level_values
    submitter_id = (
        get_lvl("jdc_person_id")+
        "v"+
        get_lvl("visit_number").astype(str)
    )
    node_df = pd.DataFrame(
        {
            "participants.submitter_id":get_lvl("jdc_person_id"),
            "submitter_id":submitter_id,
            "visit_type":time_point_df["visit_type"].replace(recode_map["visit_type"])
        })
    return node_df

def to_baseline_nodes(baseline_df):
    return {
        "participant":to_participant_node(baseline_df),
        "enrollment":to_enrollment_node(baseline_df),
        "demographic_baseline":to_demographic_baseline_node(baseline_df)
    }

def to_time_point_nodes(timepoints_df):
    return {
        "time_point":to_time_point_node(timepoints_df)
    }