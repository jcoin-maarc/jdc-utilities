"""
exports all records from the sheepdog database
in the JDC
"""

from pathlib import Path

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
option = "export"
PROD_CREDENTIALS_PATH = f"{Path(__file__).parents[1].as_posix()}/credentials.json"
PROD_COMMONS_URL = "https://jcoin.datacommons.io"
auth = Gen3Auth(endpoint=PROD_COMMONS_URL, refresh_file=PROD_CREDENTIALS_PATH)
sub = Gen3Submission(auth)
nodes = list(sub.get_dictionary_all().keys())
programs = sub.get_programs()["links"]
export_folder = Path(__file__).parents[1].joinpath("tmp/sheepdog-export-2023-06-02")
export_folder.mkdir(exist_ok=True, parents=True)

for program in programs:
    program_name = Path(program).stem
    projects = sub.get_projects(program=program_name)["links"]
    for project in projects:
        for node_name in nodes:
            project_name = Path(project).stem

            if option == "export":
                print(f"Starting {program_name}_{project_name}_{node_name}.tsv")
                node_tsv = sub.export_node(
                    program="JCOIN",
                    project=project_name,
                    node_type=node_name,
                    fileformat="tsv",
                )
                export_folder.joinpath(
                    f"{program_name}_{project_name}_{node_name}.tsv"
                ).write_text(node_tsv)
            elif option == "delete":
                print(f"Deleting {program_name},{project_name},{node_name}")
                node_tsv = sub.delete_node(
                    program=program_name, project=project_name, node_type=node_name
                )
