"""
convenience script to delete all sheepdog
records (or a large amount of nodes across all programs/projects)

"""


from pathlib import Path

from gen3.auth import Gen3Auth
from gen3.submission import Gen3Submission, Gen3SubmissionQueryError

PROD_CREDENTIALS_PATH = f"{Path(__file__).parents[1].as_posix()}/credentials.json"
PROD_COMMONS_URL = "https://jcoin.datacommons.io"
auth = Gen3Auth(endpoint=PROD_COMMONS_URL, refresh_file=PROD_CREDENTIALS_PATH)
sub = Gen3Submission(auth)

nodes = [
    "protocol",
    "core_metadata_collection",
    "participant",
    "demographic_baseline",
    "enrollment",
    "publication",
    "reference_file",
    "time_point",
    "risk_of_harm_and_consequence",
    "serious_adverse_event",
    "substance_use",
    "treatment_preference",
    "utilization_service",
    "moud_use",
    "promis",
    "justice_involvement",
    "demographic_household",
]
programs = sub.get_programs()["links"]

for program in programs:
    program_name = Path(program).stem
    projects = sub.get_projects(program=program_name)["links"]
    for project in projects:
        for node_name in reversed(nodes):
            project_name = Path(project).stem
            print(f"Deleting {program_name},{project_name},{node_name}")
            try:
                node_tsv = sub.delete_node(
                    program=program_name, project=project_name, node_name=node_name
                )
            except Gen3SubmissionQueryError as e:
                print(str(e))
