from cli import replace_ids, validate
from click.testing import CliRunner
from frictionless import Resource, Schema, validate_resource
from jsonpath_ng import parse
from submission import create_resource_validation_report

runner = CliRunner()


# replace id test

# jdc-utils replace-ids \
# --id-file C:/Users/kranz-michael/projects/test-ids/id_store/test_submitter_ids.txt \
#  --map-file C:/Users/kranz-michael/projects/test-ids/id_store/id_mappings.csv \
# --map-url C:/Users/kranz-michael/projects/test-ids-secrets.git \
# --column record_id \
# --file-path C:/Users/kranz-michael/projects/test-ids/local_files/*.csv

replace_id_args = [
    "--id-file",
    "C:/Users/kranz-michael/projects/test-ids/id_store/test_submitter_ids.txt",
    "--map-file",
    "C:/Users/kranz-michael/projects/test-ids/id_store/id_mappings.csv",
    "--map-url",
    "C:/Users/kranz-michael/projects/test-ids-secrets.git",
    "--column",
    "record_id",
    "--file-path",
    "C:/Users/kranz-michael/projects/test-ids/local_files/*.csv",
]

result = runner.invoke(replace_ids, replace_id_args)


# validate test

# jdc-utils validate --file-path baseline_valid.tsv --file-type baseline
# validate_args = [
#     '--file-path','C:/Users/kranz-michael/projects/test-ids/baseline_valid.tsv',
#     '--file-type','baseline'
# ]
# result = runner.invoke(validate,validate_args)


#


# result = runner.invoke(validate,["--schema-path",schema_path,"--file-path",file_path])
# print(result.output)
# from frictionless import Schema,Resource
# schema = Schema(schema_path)
# resource = Resource([f for f in file_path if f] ,schema=schema)
# report = create_resource_validation_report(resource)
# report = validate_resource(resource)
# file_path_exp = parse("$..resource.path[*]")
# file_paths = [x.value for x in file_path_exp.find(report)]

# file_names = " and ".join([os.path.split(f)[-1] for f in file_paths])
# errors = [task['errors'][0] for task in report['tasks'] if not report['valid']]
