import pandas as pd

from .tscores import tscore_dists
from .utility import get_propr_from_tscores


def _sum_scores(df):
    df["global"] = df[["pain_intensity"]].sum(axis=1)
    df["social"] = df[
        [
            "trouble_with_leisure",
            "trouble_with_family",
            "trouble_with_work",
            "trouble_with_activities",
        ]
    ].sum(axis=1)
    df["anxiety"] = df[
        [
            "past_week_fearful",
            "past_week_anxiety",
            "past_week_worried",
            "past_week_uneasy",
        ]
    ].sum(axis=1)
    df["cognitive"] = df[["can_concentrate", "can_remember"]].sum(axis=1)
    df["depression"] = df[
        [
            "past_week_worthless",
            "past_week_helpless",
            "past_week_depressed",
            "past_week_hopeless",
        ]
    ].sum(axis=1)
    df["fatigue"] = df[
        ["past_week_fatigued", "past_week_tired", "past_week_rundown", "fatigue_level"]
    ].sum(axis=1)
    df["pain"] = df[
        [
            "pain_daily_activity",
            "pain_work_around_house",
            "pain_social_activity",
            "pain_household_chores",
        ]
    ].sum(axis=1)
    df["physical"] = df[
        [
            "difficulty_chores",
            "difficulty_stairs",
            "difficulty_walking",
            "difficulty_traveling",
        ]
    ].sum(axis=1)
    df["sleep"] = df[
        ["sleep_quality", "sleep_refreshing", "sleep_problems", "sleep_difficulty"]
    ].sum(axis=1)
    return df[
        [
            "fatigue",
            "depression",
            "anxiety",
            "cognitive",
            "sleep",
            "physical",
            "social",
            "global",
        ]
    ]


def compute_scores(df):
    source = df.copy()
    rawscores = _sum_scores(source)
    tscores = rawscores.replace(tscore_dists)
    # compute utility scores
    scores = get_propr_from_tscores(
        t_dep=tscores["depression"],
        t_fat=tscores["fatigue"],
        t_pain=tscores["pain"],
        t_phys=tscores["physical"],
        t_slp=tscores["sleep"],
        t_sr=tscores["social"],
        t_ax=tscores["anxiety"],
        t_cog=tscores["cognitive"],
        score_pi=tscores["global"],
    )
    # return dataframe of utility scores
    # PROPr, (cognition_utility, depression_utility, fatigue_utility, pain_utility, physical_utility, sleep_utility, social_utility)
    target = df.copy()
    target["PROPr"] = scores["PROPr"]
    target["cognition_utility"] = scores["cognitive_utility"]
    target["depression_utility"] = scores["depression_utility"]
    target["fatigue_utility"] = scores["fatigue_utility"]
    target["pain_utility"] = scores["pain_utility"]
    target["physical_utility"] = scores["physical_utility"]
    target["sleep_utility"] = scores["sleep_utility"]
    target["social_utility"] = scores["social_utility"]

    return target
