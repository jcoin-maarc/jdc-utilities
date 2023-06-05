"""
submit all modified (edited) records
"""
import shutil
from pathlib import Path

import pandas as pd
from gen3.auth import Gen3Auth
from gen3.submission import Gen3Submission

PROD_CREDENTIALS_PATH = f"{Path(__file__).parents[1].as_posix()}/credentials.json"
PROD_COMMONS_URL = "https://jcoin.datacommons.io"
auth = Gen3Auth(endpoint=PROD_COMMONS_URL, refresh_file=PROD_CREDENTIALS_PATH)
sub = Gen3Submission(auth)

nodes = [
    "participant",
    "enrollment",
    "demographic",
    "time_point",
    "core_metadata_collection",
    "reference_file",
]
edits_path = (
    Path(__file__).parents[1].joinpath("tmp/sheepdog-edits-from-export-2023-06-02")
)
export_path = edits_path.with_name("sheepdog-export-2023-06-02")
submit_path = edits_path.with_name("sheepdog-submit-2023-06-05")
submit_path.mkdir(exist_ok=True)
for node_name in nodes:
    export_file_paths = export_path.glob(f"*{node_name}.tsv")
    for export_file_path in export_file_paths:
        edits_file_path = edits_path / export_file_path.name
        submit_file_path = submit_path / export_file_path.name
        if edits_file_path.is_file():
            shutil.copyfile(edits_file_path, submit_file_path)
        else:
            shutil.copyfile(export_file_path, submit_file_path)

        file_components = submit_file_path.name.split("_")
        program_name = file_components[0]
        project_name = file_components[1]
        node_name = file_components[2]

        sub.submit_file(project_id=f"{program_name}-{project_name}", filename=str(path))
