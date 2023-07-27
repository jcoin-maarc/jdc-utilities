import os
from pathlib import Path

import pandas as pd

# frictionless plugins
from dataforge.frictionless import encode_table

# frictionless
from frictionless import Package, Resource, steps, transform, validate

from jdc_utils import core_measures
from jdc_utils.promis import schemas
from jdc_utils.promis.tscores import raw_to_tscore_map
from jdc_utils.promis.utility import get_propr_from_tscores


class Promis:
    def __init__(self, data_package_descriptor):
        start_path = os.getcwd()
        if isinstance(data_package_descriptor, (str, os.PathLike)):
            path = Path(data_package_descriptor)
            os.chdir(path.parent)
            core_measures = Package(path.name)

        baseline_resource = core_measures.get_resource("baseline")
        timepoints_resource = core_measures.get_resource("timepoints")

        self.package = Package()
        self.add_baseline(baseline_resource)
        self.add_timepoints(timepoints_resource)

        os.chdir(start_path)

    def add_baseline(self, resource):
        # filter measures from promis.baseline
        # add resource to package
        name = "jcoin-baseline"
        schema = schemas.baseline
        fieldnames = [f["name"] for f in schema["fields"]]
        df = resource.to_petl().todf()
        baseline_resource = Resource(name=name, data=df[fieldnames], schema=schema)
        self.package.add_resource(baseline_resource)

    def add_timepoints(self, resource):
        # filter Record Linkage and promis
        # encode promis measures with ints
        # compute scores
        # add schema
        # add resource to package
        name = "jcoin-timepoints-promis"
        schema = schemas.timepoints_promis
        fieldnames = [f["name"] for f in schema["fields"]]
        individual_scores_resource = resource.transform(
            steps=[
                encode_table(
                    encodings=core_measures.encodings.fields,
                    reservecodes=None,
                ),
                steps.field_filter(names=fieldnames),
            ]
        )
        promis_df = compute_scores(individual_scores_resource)
        promis_resource = Resource(name=name, data=promis_df, schema=schema)
        self.package.add_resource(promis_resource)

    def write(self, outdir=None):
        if outdir:
            self.outdir = outdir

        self.written_package = Package()
        Path(outdir).mkdir(exist_ok=True, parents=True)
        os.chdir(outdir)
        Path("schemas").mkdir(exist_ok=True)
        Path("data").mkdir(exist_ok=True)

        # write csv datasets and validation report
        for resource in self.package["resources"]:
            csvpath = f"data/{resource['name']}.csv"
            schemapath = f"schemas/{resource['name']}.json"

            resource.schema.to_json(schemapath)
            resource.to_petl().tocsv(csvpath)

            self.written_package.add_resource(
                Resource(name=resource["name"], path=csvpath, schema=schemapath)
            )

        self.written_package.to_json(f"data-package.json")
        self.written_package_report = validate("data-package.json")
        self.written_package_report.to_json("report.json")
        Path("report-summary.txt").write_text(self.written_package_report.to_summary())

    def submit():
        pass


def _sum_scores(df):
    cols = dict(
        # global_pain=["pain_intensity"], # not used in calcs
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
    sum_cols_if_no_missing = lambda colnames: df[colnames].sum(
        axis=1, min_count=len(cols)
    )

    for sumname, colnames in cols.items():
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
    rawscores = _sum_scores(
        resource.to_pandas()
    )  # NOTE: to_pandas() sets to logical (eg Missing to NA)
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
        # t_cog=tscores["cognitive"],
        score_pi=tscores["global_pain"],
    )
    # return a copy of the resource data (dataframe) of utility scores
    # PROPr, (cognition_utility, depression_utility, fatigue_utility, pain_utility, physical_utility, sleep_utility, social_utility)
    if hasattr(resource, "data"):
        target = resource.data.copy()
    else:
        target = resource.to_petl().todf()

    target = source.copy()
    target["PROPr"] = scores["PROPr"]
    target["cognition_utility"] = scores["cognitive_utility"]
    target["depression_utility"] = scores["depression_utility"]
    target["fatigue_utility"] = scores["fatigue_utility"]
    target["pain_utility"] = scores["pain_utility"]
    target["physical_utility"] = scores["physical_utility"]
    target["sleep_utility"] = scores["sleep_utility"]
    target["social_utility"] = scores["social_utility"]
    target.fillna("Missing", inplace=True)
    return target
