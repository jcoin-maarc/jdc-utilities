"""
Generate,validate, and submit a core measure data package
"""
import copy
import datetime

# get schemas and validate files, creating report,writing datasets, and package metadata
import os
import re
import time

# base python utils
from collections import abc
from functools import reduce
from pathlib import Path

import dataforge.frictionless
import pandas as pd

# frictionless plugins
from dataforge.frictionless import encode_table

# frictionless
from frictionless import Package, Resource, transform, validate

from jdc_utils import schemas
from jdc_utils.encoding import core_measures as encodings
from jdc_utils.submission.general import submit_package_to_jdc
from jdc_utils.transforms import add_missing_fields, deidentify, to_new_names
from jdc_utils.transforms.sheepdog import to_baseline_nodes, to_time_point_nodes
from jdc_utils.utils import map_to_sheepdog, read_package, zip_package


class CoreMeasures:
    """
    Object that takes in a path-like object pointing to data file(s)
    or anything accepted by the frictionless package object
    (e.g., a datapackage.json) containing the paths to resources (filepath).

    Parameters
    ----------
    id_file: Optional[str]
        The generated ids (see `replace_id` function for usage)
    id_column: Optional[str]
        ID column(s) for deidentification functions (see `replace_ids` and `shift_date` functions)
    history_path: Optional[str]
        Directory containing all version control history of mapping files
        (i.e., git bare repos)
    date_columns: Optional[str]
        The specified date columns for `shift_dates` function
        (if none, will default to all date column types in df. if no date column types, then will not convert anything)
    outdir: Optional[str]
        Directory to write core measure package
    """

    def __init__(
        self,
        filepath=None,
        id_file=None,
        id_column=None,
        history_path=None,
        date_columns=None,
        outdir=None,
        transform_steps=[
            "add_new_names",
            "add_missing_fields",
            "replace_ids",
            "shift_dates",
        ],
    ):
        # resolve paths just in case directories change
        # note, check that this is a directory in case the input
        # is something else like a url that is valid input to Package
        # NOTE: should all be pathlike
        self.id_file = _resolve_if_path(id_file)
        self.id_column = id_column
        self.history_path = _resolve_if_path(history_path)
        self.date_columns = date_columns
        self.outdir = _resolve_if_path(outdir)
        self.basedir = pwd = os.getcwd()
        self.transform_steps = transform_steps
        self.package = Package()

    @staticmethod
    def __add_new_names(df, schema):
        fields = schema["fields"]
        mappings = {
            field["custom"]["jcoin:original_name"]: field["name"] for field in fields
        }
        return to_new_names(df=df, mappings=mappings)

    @staticmethod
    def __add_missing_fields(df, schema):
        field_list = [field["name"] for field in schema["fields"]]
        return add_missing_fields(df, field_list, missing_value="Missing")

    def _add_resource(
        self,
        df_or_path,
        name,
        schema,
        transform_steps=[
            "add_new_names",
            "add_missing_fields",
            "replace_ids",
            "shift_dates",
        ],
    ):
        if isinstance(df_or_path, pd.DataFrame):
            df = df_or_path
        elif isinstance(df_or_path, (str, os.PathLike)):
            df = Resource(path=str(df_or_path)).to_petl().todf()

        deidentify_fxns = (
            []
        )  # have dependencies so are bundled together in its own wrapper function
        fxns = []
        for trans in transform_steps:
            if trans == "add_new_names":
                fxns.append((self.__add_new_names, {"schema": schema}))
            elif trans == "add_missing_fields":
                fxns.append((self.__add_missing_fields, {"schema": schema}))
            elif trans == "replace_ids" or trans == "shift_dates":
                deidentify_fxns.append(trans)

        # if deidentify functions listed, add to the function list
        if deidentify_fxns:
            deidentify_params = {
                "id_file": self.id_file,
                "id_column": self.id_column,
                "history_path": self.history_path,
                "date_columns": self.date_columns,
                "fxns": deidentify_fxns,
            }
            fxns.append((deidentify, deidentify_params))

        # chain through selected functions
        newdf = reduce(lambda _df, fxn: fxn[0](_df, **fxn[-1]), fxns, df)
        # make resource
        resource = Resource(name=name, data=newdf, schema=schema, format="pandas")

        self.package.add_resource(resource)

    def add_baseline(self, df_or_path):
        name = "baseline"
        schema = schemas.core_measures.baseline
        steps = self.transform_steps
        self._add_resource(df_or_path, name, schema, steps)

    def add_timepoints(self, df_or_path):
        name = "timepoints"
        schema = schemas.core_measures.timepoints
        steps = self.transform_steps
        self._add_resource(df_or_path, name, schema, steps)

    def add_staff_baseline(self, df_or_path):
        name = "staff-baseline"
        schema = schemas.core_measures.staff_baseline
        steps = self.transform_steps
        self._add_resource(df_or_path, name, schema, steps)

    def add_staff_timepoints(self, df_or_path):
        name = "staff-timepoints"
        schema = schemas.core_measures.staff_timepoints
        steps = self.transform_steps
        self._add_resource(df_or_path, name, schema, steps)

    def write(self, outdir=None):
        """
        writes package to core measure format
        NOTE: use kwargs to pass in all package (ie hub)
        specific package properties (title,name,desc etc)
        """
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

        # write SPSS/Stata files if valid package
        if self.written_package_report["valid"]:
            ## copy package so trgt pckgs not added to iterator
            sourcepackage = self.written_package.to_copy()
            for source in sourcepackage["resources"]:
                source_path = Path(source["path"])

                target_spss_path = f"data/{source['name']}.sav"
                target_stata_path = f"data/{source['name']}.dta"
                target_spss_schemapath = f"schemas/{source['name']}-sav.json"
                target_stata_schemapath = f"schemas/{source['name']}-dta.json"

                # TODO: test to see if instantiating new Resource is needed (may not be given the iterator is now copied)
                target_spss = Resource(
                    path=str(source.path), schema=dict(source.schema)
                )
                target_spss = target_spss.transform(
                    steps=[
                        encode_table(
                            encodings=encodings.fields,
                            reservecodes=encodings.reserve["spss"],
                        )
                    ]
                )
                # TODO: test to see if instantiating new Resource is needed (may not be given the iterator is now copied)
                target_stata = Resource(
                    path=str(source.path), schema=dict(source.schema)
                )
                target_stata = target_stata.transform(
                    steps=[
                        encode_table(
                            encodings=encodings.fields,
                            reservecodes=encodings.reserve["stata"],
                        )
                    ]
                )

                target_spss.infer()
                target_stata.infer()

                target_spss.write(target_spss_path)
                target_stata.write(target_stata_path)
                target_spss.schema.to_json(target_spss_schemapath)
                target_stata.schema.to_json(target_stata_schemapath)

                target_resource_spss = Resource(
                    name=f"{source['name']}-sav",
                    title="SPSS (.sav) dataset",
                    description="This is an annotated SPSS dataset. To see the schema with the value labels (encoding) and variable labels (title), see `schemas/<tablename>-sav.json`",
                    path=target_spss_path,
                    # schema=target_spss_schemapath #No validation/read stream yet (need to add to dataforge)
                )
                target_resource_stata = Resource(
                    name=f"{source['name']}-dta",
                    title="Stata (.dta) dataset",
                    description="This is an annotated Stata dataset. To see the schema with the value labels (encoding) and variable labels (title), see `schemas/<tablename>-sav.json`",
                    path=target_stata_path,
                    # schema=target_stata_schemapath #No validation/read stream yet (need to add to dataforge)
                )
                self.written_package.add_resource(target_resource_spss)
                self.written_package.add_resource(target_resource_stata)

            self.written_package.to_json("data-package.json")

        else:
            print(
                f"Package not valid so not generating spss and stata files. Check report summary"
            )

        os.chdir(self.basedir)

        return self

    def zip(self, pkgpath=None, zipdir=None):
        if not zipdir:
            zipdir = Path(self.outdir).parent

        if not pkgpath:
            pkgpath = self.outdir

        self.zipped_package_path = zip_package(pkgpath, zipdir)

        return self

    def submit(
        self,
        commons_project_code,
        commons_file_guid,
        commons_file_submitter_id,
        commons_credentials_path="credentials.json",
        zipped_package_path=None,
    ):
        """
        submission and mapping to sheepdog and file upload

        Note, this function assumes there exists a record already in the
        commons for this particular hub's core measure data package.

        If this is a first time upload, please upload with gen3 client
        and map via gui or use following function:

        ```python
        from jdc_utils.submission import submit_package_to_jdc

        gen3_file = submit_package_to_jdc(
            package_path=zip_path,
            commons_project=commons_project,
            commons_file_submitter_id=commons_file_submitter_id,
            sheepdog_data_type="Interview",
            sheepdog_data_category="Core Measures",
            sheepdog_data_format="ZIP",
            submission_type="create",
            credentials_path=credentials_path,
        )
        ```
        Parameters
        -----------------
        commons_project_code (str): project code for authorization service
        commons_file_guid: file generated unique id for the indexed file in commons
        commons_file_submitter_id: the submitter-created id that is a part of the service exposing metadata in data portal (ie sheepdog)
        credentials_path: credentials with key and API token giving access for the set of permissions
        zipped_package_path: the path to the to-be-uploaded zipped file


        """
        # compress package directory to zip
        if zipped_package_path:
            assert (
                Path(zipped_package_path).suffix == ".zip"
            ), "Must be a zipped/compressed file"
            self.zipped_package_path = zipped_package_path
        elif hasattr(self, "zipped_package_path"):
            zipped_package_path = self.zipped_package_path
        else:
            self.zip()

        # submit the zipped package to JDC
        gen3_file = submit_package_to_jdc(
            package_path=self.zipped_package_path,
            commons_project=commons_project_code,
            file_guid=commons_file_guid,
            sheepdog_file_submitter_id=commons_file_submitter_id,
            submission_type="update",
            credentials_path=commons_credentials_path,
        )

        # map subset of variables to sheepdog to expose in data explorer dashboard
        baseline_df = self.package.get_resource("baseline").to_pandas()
        baseline_df["role_in_project"] = "Client"  # TODO: add staff
        timepoints_df = self.package.get_resource("timepoints").to_pandas()

        # get list of people not in baseline but in timepoints
        isin_baseline = timepoints_df.index.get_level_values("jdc_person_id").isin(
            baseline_df.index.get_level_values("jdc_person_id")
        )
        missing_ppl = timepoints_df.loc[~isin_baseline]

        timestamp = time.time()
        date_time = datetime.date.fromtimestamp(timestamp)
        str_date_time = date_time.strftime("%Y%m%d")

        Path(f"missing_participants_{str_date_time}.txt").open(mode="a").write(
            f"Sheepdog upload at {str_date_time}:\n"
            + "The following people are in time points but not in baseline"
            + "and therefore not added to the time_points node in sheepdog model\n\n"
            + "\n".join(missing_ppl.index.get_level_values("jdc_person_id"))
        )

        last_node_output = map_core_measures_to_sheepdog(
            baseline_df=baseline_df,
            timepoints_df=timepoints_df.loc[isin_baseline],
            program="JCOIN",
            project=commons_project_code,
            credentials_path=commons_credentials_path,
        )


def map_core_measures_to_sheepdog(
    baseline_df,
    timepoints_df,
    program,
    project,
    credentials_path,
    node_list=None,
    delete_first=True,
):
    """
    Map core measure variables to existing sheepdog model

    baseline_df: pandas dataframe of logical representation (ie True/False boolean instead of Yes/No; all missing values are NA) of  validated core measure package
     NOTE: the `current_project_status` column MUST be added as it is not a part of the core measures currently.
    timepoints_df: same as baseline_df but for the time points dataset
    staff_df: TODO: NOT YET ADDED
    program: see map_to_sheepdog
    project: see map_to_sheepdog
    credentials_path: see map_to_sheepdog
    node_list: see map_to_sheepdog
    delete_first: see map_to_sheepdog
    """

    endpoint = "https://jcoin.datacommons.io/"

    baseline_node_data = to_baseline_nodes(baseline_df)
    timepoints_node_data = to_time_point_nodes(timepoints_df)
    sheepdog_data = {**baseline_node_data, **timepoints_node_data}

    map_to_sheepdog(
        sheepdog_dfs=sheepdog_data,
        endpoint=endpoint,
        program=program,
        project=project,
        credentials_path=credentials_path,
        node_list=node_list,
        delete_first=delete_first,
    )


def _resolve_if_path(var):
    if var:
        if os.path.isdir(var) or os.path.isfile(var):
            return str(Path(var).resolve())
        else:
            return var
    else:
        return var
