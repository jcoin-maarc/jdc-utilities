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
    "gender_identity":{
        "Transgender man/trans man/female-to-male (FTM)":"Transgender",
        "Transgender woman/trans woman/male-to-female (MTF)":"Transgender",
        "Genderqueer/gender nonconforming/neither exclusively male nor female":"Gender nonconforming",
        "Additional gender category (or other)":"Something else",
        "Missing":"Not reported",
        "Legitimately skipped":"Not reported"
    }
}

def _choose_gender_field(df):
    if (df["gender_identity_condensed"]=="Missing" or df["gender_identity_condensed"]=="Legitimately skipped")<len(df):
        return df["gender_identity_condensed"]
    else:
        return (
            df["gender_identity"]
            .replace(recode_map["gender"])
        )

def to_participant_node(baseline_df):
    """
    Takes the validated core measure baseline 
    dataset from core measure pacakge and return 
    a dataframe for participant node of JDC sheepdog data model
    """ 
    node_df = pd.DataFrame({
        "protocol.submitter_id":"main",
        "submitter_id":baseline_df["jdc_person_id"],
        "role_in_study":baseline_df["role_in_study"],
        "quarter_recruited":baseline_df["quarter_enrolled"],
        "current_client_status":baseline_df["current_study_status"]
    })
    return node_df

def to_demographic_baseline_node(baseline_df):

    node_df = pd.DataFrame({
        "participants.submitter_id":baseline_df["jdc_person_id"],
        "submitter_id":baseline_df["jdc_person_id"],
        "gender":_choose_gender_field(baseline_df),
        "hispanic":baseline_df["hispanic_latino"],
        "race":collapse_checkall(df,**race_map),
    })
    return node_df

def to_enrollment_node(baseline_df):

    node_df = pd.DataFrame({
        "participants.submitter_id":baseline_df["jdc_person_id"],
        "submitter_id":baseline_df["jdc_person_id"]
    })
    return node_df

def to_time_point_node(time_point_df):

    node_df = pd.DataFrame(
        {
            "participants.submitter_id":timepoints_df["jdc_person_id"],
            "submitter_id":timepoints_df["jdc_person_id"]+"v"+timepoints_df["visit_number"],
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
        "time_point":to_time_point_node(time_point_df)
    }