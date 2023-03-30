def to_nodes(package):
    def _choose_gender_field(df):
            if (df["gender_identity_condensed"]=="Missing")<len(df):
                return df["gender_identity_condensed"]
            else:
                return (
                    df["gender_identity"]
                    .replace(recode_map["gender"])
                )
        
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

        }
    }
    node_records = {
            "participant":{
                "protocol.submitter_id":"main",
                "submitter_id":baseline_df["jdc_person_id"],
                "role_in_study":baseline_df["role_in_study"],
                "quarter_recruited":baseline_df["quarter_enrolled"],
                "current_client_status":baseline_df["current_study_status"],
            "demographics_baseline":{
                "participants.submitter_id":baseline_df["jdc_person_id"],
                "submitter_id":baseline_df["jdc_person_id"],
                "gender":_choose_gender_field(baseline_df),
                "hispanic":baseline_df["hispanic_latino"],
                "race":collapse_checkall(df,**race_map),
            "enrollment":{
                "participants.submitter_id":baseline_df["jdc_person_id"],
                "submitter_id":baseline_df["jdc_person_id"]
            },
            "time_point":{
                "participants.submitter_id":timepoints_df["jdc_person_id"],
                "submitter_id":timepoints_df["jdc_person_id"]+"v"+timepoints_df["visit_number"],
                "visit_type":timepoints_df["visit_type"].replace(recode_map["visit_type"]),
                "visit_number":timepoints_df["visit_number"]
            }
            }
        }}