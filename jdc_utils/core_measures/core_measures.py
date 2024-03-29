"""
Generate,validate, and submit a core measure data package
"""
import copy
import datetime
import os
import re
import time
from collections import abc
from functools import reduce
from pathlib import Path

import dataforge.frictionless
import pandas as pd

# frictionless plugins
from dataforge.frictionless import encode_table

# frictionless
from frictionless import Package, Resource, transform, validate

# general functions
from jdc_utils.submission import submit_package_to_jdc
from jdc_utils.transforms import add_missing_fields, deidentify, to_new_names

# general utilities
from jdc_utils.utils.gen3 import map_to_sheepdog
from jdc_utils.utils.packaging import read_package, zip_package

# core measure modules
from . import encodings, schemas, sheepdog, derived_measures


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
    transform_steps: the transformation functions to be applied (and the order of) upon adding a data resource.
        Currently available functions include: [
            "sync_new_names" ,
            "add_missing_fields",
            "replace_ids",
            "shift_dates",
        ],
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
            "sync_new_names",# same as `"add_new_names"` but both kept for bckwards compat
            "add_missing_fields",
            "replace_ids",
            "shift_dates",
        ],
        **kwargs
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
        self.sheepdog_package = Package()

        self.schemas = schemas

    # user facing functions to build core measure data package, writing the package, and submitting to JDC
    def add_baseline(self, df_or_path):
        name = "baseline"
        schema = self.schemas.baseline
        steps = self.transform_steps
        resource = self._generate_resource(df_or_path, name, schema, steps)

        # derived measures
        resource.data = (
            pd.DataFrame(resource.data)
            .pipe(derived_measures.combine_race)
            .pipe(derived_measures.map_gender_id_condensed)

        )
        self.package.add_resource(resource)

    def add_timepoints(self, df_or_path):
        name = "timepoints"
        schema = self.schemas.timepoints
        steps = self.transform_steps
        resource = self._generate_resource(df_or_path, name, schema, steps)
        # derived measures
        #resource.data = derived_measures.promis.compute_scores(resource)
        self.package.add_resource(resource)

    def add_staff_baseline(self, df_or_path):
        name = "staff-baseline"
        schema = self.schemas.staff_baseline
        steps = self.transform_steps
        resource = self._generate_resource(df_or_path, name, schema, steps)
        resource.data = (
            pd.DataFrame(resource.data)
            .pipe(derived_measures.combine_race)
            .pipe(derived_measures.map_gender_id_condensed)

        )
        self.package.add_resource(resource)

    def add_staff_timepoints(self, df_or_path):
        name = "staff-timepoints"
        schema = self.schemas.staff_timepoints
        steps = self.transform_steps
        resource = self._generate_resource(df_or_path, name, schema, steps)
        self.package.add_resource(resource)

    # NOTE: below are temporary and will change if DD becomes more like frictionelss
    # core measure data model
    def convert_baseline_to_sheepdog(self):
        baseline_df = pd.concat(
            [
                self.package.get_resource(name).to_pandas().assign(name=name)
                for name in ["baseline", "staff-baseline"]
                if name in self.package.resource_names
            ]
        )
        role_map = {"baseline": "Client", "staff-baseline": "Staff"}
        baseline_df["role_in_project"] = baseline_df["name"].replace(role_map)
        baseline_df[
            "projects.code"
        ] = self.commons_project_code  # protocol node was removed
        baseline_node_data = sheepdog.to_baseline_nodes(baseline_df)

        self.sheepdog_package.resources.extend(baseline_node_data)

    def convert_timepoints_to_sheepdog(self):
        assert self.package.get_resource("baseline")
        baseline_df = self.package.get_resource("baseline").to_pandas()
        timepoints_df = self.package.get_resource("timepoints").to_pandas()

        # get list of people not in baseline but in timepoints
        # TODO: add
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

        timepoints_node_data = sheepdog.to_time_point_nodes(
            timepoints_df.loc[isin_baseline]
        )

        for resource in timepoints_node_data:
            self.sheepdog_package.add_resource(resource)

    def write(self, outdir=None):
        """
        writes package to core measure format
        NOTE: use kwargs to pass in all package (ie hub)
        specific package properties (title,name,desc etc)
        """
        if outdir:
            self.outdir = outdir
        else:
            outdir = self.outdir

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
        if not self.written_package_report["valid"]:
            print("WARNING: package not valid, see the report-summary.txt file")

        
        ## copy package so trgt pckgs not added to iterator
        sourcepackage = self.written_package.to_copy()
        for source in sourcepackage["resources"]:
            source_path = Path(source["path"])

            target_spss_path = f"data/{source['name']}.sav"
            target_stata_path = f"data/{source['name']}.dta"
            # target_spss_schemapath = f"schemas/{source['name']}-sav.json"
            # target_stata_schemapath = f"schemas/{source['name']}-dta.json"

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
            # target_spss.schema.to_json(target_spss_schemapath)
            # target_stata.schema.to_json(target_stata_schemapath)

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
        **kwargs
    ):
        """
        submission and mapping to sheepdog and file upload.

        If `commons_file_guid` (aka the object id etc) is specified,
        assumes you are updating an exisiting record. If specified as None,
        it will create a new record with a newly minted guid.

        NOTE: Both options have prompts to make sure you want to continue with upload.


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

        if commons_file_guid:
            confirmation = input(
                f"Submit an updated version of an existing data package with {commons_file_guid}? Y/N"
            )
            submission_type = "update"
            if confirmation.strip().lower() == "y":
                # submit the zipped package to JDC
                gen3_file = submit_package_to_jdc(
                    package_path=self.zipped_package_path,
                    commons_project=commons_project_code,
                    file_guid=commons_file_guid,
                    sheepdog_file_submitter_id=commons_file_submitter_id,
                    submission_type=submission_type,
                    credentials_path=commons_credentials_path,
                )
            else:
                raise Exception("Aborted JDC submission process")        
        else:
            confirmation = input(f"Submit a new file? BE CAREFUL: this will create a new guid. Y/N")
            
            submission_type = "create"
            if confirmation.strip().lower() == "y":
                gen3_file = submit_package_to_jdc(
                    package_path=self.zipped_package_path,
                    commons_project=commons_project_code,
                    sheepdog_file_submitter_id=commons_file_submitter_id,
                    sheepdog_data_type="Interview",
                    sheepdog_data_category="Core Measures",
                    sheepdog_data_format="ZIP",
                    submission_type=submission_type,
                    sheepdog_other_cmc_node_metadata={"title": "Core Measures"},
                    credentials_path=commons_credentials_path,
                )
            else:
                raise Exception("Aborted JDC submission process")

        # after file uploaded and mapped, map the data to sheepdog
        self.map_to_sheepdog(commons_project_code, commons_credentials_path)

    def map_to_sheepdog(
        self,
        commons_project_code,
        commons_credentials_path,
        node_list=None,
        delete_first=True,
        commons_program="JCOIN",
        endpoint="https://jcoin.datacommons.io/",
    ):
        """
        Map core measure variables to existing sheepdog model

        sheepdog_package: package containing resources representing a gen3 node
        NOTE: the `current_project_status` column MUST be added as it is not a part of the core measures currently.
        timepoints_sheepdog_resource: same as baseline_df but for the time points dataset
        program: see map_to_sheepdog
        project: see map_to_sheepdog
        credentials_path: see map_to_sheepdog
        node_list: see map_to_sheepdog
        delete_first: see map_to_sheepdog
        """
        self.commons_project_code = commons_project_code
        if not self.sheepdog_package:
            self.convert_baseline_to_sheepdog()
            self.convert_timepoints_to_sheepdog()

        last_node_output = map_to_sheepdog(
            sheepdog_package=self.sheepdog_package,
            endpoint=endpoint,
            program=commons_program,
            project=commons_project_code,
            credentials_path=commons_credentials_path,
            node_list=node_list,
            delete_first=delete_first,
        )

    @staticmethod
    def __add_new_names(df, schema):
        fields = schema["fields"]
        mappings = {field.get("custom").get("jcoin:original_name"): field["name"] for field in fields 
            if field.get("custom",{}).get("jcoin:original_name")}
        return to_new_names(df=df, mappings=mappings)

    @staticmethod
    def __add_missing_fields(df, schema):
        field_list = [field["name"] for field in schema["fields"]]
        return add_missing_fields(df, field_list, missing_value="Missing")

    # below are internal functions to be called by the user facing methods add_<resource name>() and submit()
    def _generate_resource(
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
        fxns = {}  # NOTE: dicts are ordered now in python
        for trans in transform_steps:
            if trans == "add_new_names" or trans == "sync_new_names":
                fxns[trans] = (self.__add_new_names, {"schema": schema})
            elif trans == "add_missing_fields":
                fxns[trans] = (self.__add_missing_fields, {"schema": schema})
            elif trans == "replace_ids" or trans == "shift_dates":
                deidentify_fxns.append(trans)
                fxns["deidentify"] = None

        # if deidentify functions listed, add to the function list
        if deidentify_fxns:
            deidentify_params = {
                "id_file": self.id_file,
                "id_column": self.id_column,
                "history_path": self.history_path,
                "date_columns": self.date_columns,
                "fxns": deidentify_fxns,
            }
            fxns["deidentify"] = (deidentify, deidentify_params)

        # chain through selected functions
        newdf = reduce(lambda _df, fxn: fxn[0](_df, **fxn[-1]), fxns.values(), df)
        newdf.fillna("Missing",inplace=True)
        # make resource
        resource = Resource(name=name, data=newdf, schema=schema, format="pandas")
        return resource


def _resolve_if_path(var):
    if var:
        if os.path.isdir(var) or os.path.isfile(var):
            return str(Path(var).resolve())
        else:
            return var
    else:
        return var
