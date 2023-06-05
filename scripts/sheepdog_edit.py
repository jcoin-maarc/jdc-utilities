"""
modifies all exported node records
to a new data dictionary with breaking changes in preparation
for  re-submitting records after new data dictionary is
deployed
"""

from pathlib import Path

import pandas as pd
from gen3.auth import Gen3Auth
from gen3.submission import Gen3Submission

# After sheepdog mapping change:
# TODO: add title to all core_metadata_collection records ()
# TODO: delete "Visit" from visit_type
# TODO: (Phil to do) add protocol to project records
# TODO: change protocol.submitter_id to project.code with project name
# TODO: from demographic.race: delete " (SPECIFY.*" regexular exp

# FUTURE:
# TODO: to core measure packages: add visit_month (sheepdog mapping all ready)
# TODO:add core_metadata_collection.date_file_uploaded NOTE: will do for new uploads
PROD_CREDENTIALS_PATH = f"{Path(__file__).parents[1].as_posix()}/credentials.json"
PROD_COMMONS_URL = "https://jcoin.datacommons.io"
auth = Gen3Auth(endpoint=PROD_COMMONS_URL, refresh_file=PROD_CREDENTIALS_PATH)
sub = Gen3Submission(auth)
nodes = list(sub.get_dictionary_all().keys())
programs = sub.get_programs()["links"]
export_folder = Path(__file__).parents[1].joinpath("tmp/sheepdog-export-2023-06-02")
edits_folder = export_folder.with_name("sheepdog-edits-from-export-2023-06-02")
edits_folder.mkdir(exist_ok=True)
for program in programs:
    program_name = Path(program).stem
    projects = sub.get_projects(program=program_name)["links"]
    for project in projects:
        for node_name in nodes:
            project_name = Path(project).stem
            tsvpath = export_folder.joinpath(
                f"{program_name}_{project_name}_{node_name}.tsv"
            )
            try:
                node_df = pd.read_csv(tsvpath, dtype="string", sep="\t")
            except:
                edits_folder.joinpath("read-error.txt").write_text(str(tsvpath) + "\n")

            orig_df = node_df.copy()

            is_change = False
            if "protocols.submitter_id" in node_df and not node_df.empty:
                is_change = True
                node_df.rename(
                    columns={"protocols.submitter_id": "projects.code"}, inplace=True
                )
                node_df["projects.code"] = project_name

            if node_name == "core_metadata_collection" and not node_df.empty:
                is_change = True
                node_df["title"] = (
                    node_df["submitter_id"].replace("_|-", " ", regex=True).str.title()
                )

            if node_name == "demographic_baseline":
                node_name = "demographic"
                node_df["type"] = node_name

            if "visit_type" in node_df and not node_df.empty:
                is_change = True
                node_df["visit_type"].replace(" Visit", "", regex=True, inplace=True)

            if "race" in node_df and not node_df.empty:
                is_change = True
                node_df["race"].replace(" \(SPECIFY.*", "", regex=True, inplace=True)

            # change column names
            changed_names = {
                "quarter_recruited": "quarter_enrolled",
                "current_client_status": "current_study_status",
                "state_of_client_enrollment": "state_of_enrollment",
            }
            node_df.rename(columns=changed_names, inplace=True)
            if is_change:
                for col in node_df:
                    if node_df[col].isna().sum() == len(node_df[col]):
                        del node_df[col]
                node_df.to_csv(
                    edits_folder.joinpath(
                        f"{program_name}_{project_name}_{node_name}.tsv"
                    ),
                    sep="\t",
                    index=False,
                )
