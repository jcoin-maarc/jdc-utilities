import pandas as pd

from .tscores import raw_to_tscore_map
from .utility import get_propr_from_tscores

def _sum_scores(df):
    cols = dict(
        global_pain=["pain_intensity"],
        anxiety=[
            "past_week_fearful",
            "past_week_anxiety",
            "past_week_worried",
            "past_week_uneasy",
        ],
        social=[
            "trouble_with_leisure",
            "trouble_with_family",
            "trouble_with_work",
            "trouble_with_activities",
        ],
        cognitive=["can_concentrate", "can_remember"],
        depression=[
            "past_week_worthless",
            "past_week_helpless",
            "past_week_depressed",
            "past_week_hopeless",
        ],
        fatigue=[
            "past_week_fatigued",
            "past_week_tired",
            "past_week_rundown",
            "fatigue_level",
        ],
        pain=[
            "pain_daily_activity",
            "pain_work_around_house",
            "pain_social_activity",
            "pain_household_chores",
        ],
        physical=[
            "difficulty_chores",
            "difficulty_stairs",
            "difficulty_walking",
            "difficulty_traveling",
        ],
        sleep=[
            "sleep_quality",
            "sleep_refreshing",
            "sleep_problems",
            "sleep_difficulty",
        ],
    )
    sum_cols_if_no_missing = lambda colnames: df[colnames].sum(axis=1, min_count=len(cols))

    for sumname,colnames in cols.items():
        df[sumname] = sum_cols_if_no_missing(colnames)

    return df[cols.keys()]


def compute_scores(resource) -> pd.DataFrame:
    """ 
    computes scores using the code and methodology from:
    implementation of propr score for PROMIS
    https://doi.org/10.1177/0272989X18776637 (Dewitt et al 2018).
    taken from https://raw.githubusercontent.com/janelhanmer/PROPr/master/propr.py

    NOTE: currently if a missing variable, doesn't compute score.
    TODO: impute missing values (or at least have option?)
    """
    source = df.to_pandas().copy() # NOTE: set to logical (eg Missing to NA)
    rawscores = _sum_scores(source)
    tscores = rawscores.replace(raw_to_tscore_map)
    # compute theta utility scores
    scores = get_propr_from_tscores(
        t_dep=tscores["depression"],
        t_fat=tscores["fatigue"],
        t_pain=tscores["pain"],
        t_phys=tscores["physical"],
        t_slp=tscores["sleep"],
        t_sr=tscores["social"],
        t_ax=tscores["anxiety"],
        t_cog=tscores["cognitive"],
        score_pi=tscores["global_pain"],
    )
    # return a copy of the resource data (dataframe) of utility scores
    # PROPr, (cognition_utility, depression_utility, fatigue_utility, pain_utility, physical_utility, sleep_utility, social_utility)
    if hasattr(resource,"data"):
        target = resource.data.copy()
    else:
        target = resource.to_petl().todf()

    target["PROPr"] = scores["PROPr"]
    target["cognition_utility"] = scores["cognitive_utility"]
    target["depression_utility"] = scores["depression_utility"]
    target["fatigue_utility"] = scores["fatigue_utility"]
    target["pain_utility"] = scores["pain_utility"]
    target["physical_utility"] = scores["physical_utility"]
    target["sleep_utility"] = scores["sleep_utility"]
    target["social_utility"] = scores["social_utility"]

    return target
