from click.testing import CliRunner
from cli import validate 
from submission import create_resource_validation_report
from frictionless import validate_resource,Resource,Schema
from jsonpath_ng import parse
schema_path = r"C:\Users\kranz-michael\projects\frictionless-jcoin\hubs\metadata\table_schemas\table-schema-baseline.json"
#file_path = (r"C:\Users\kranz-michael\projects\frictionless-jcoin\hubs\data\tests\test1.tsv",r"C:\Users\kranz-michael\projects\frictionless-jcoin\hubs\data\tests\test1.tsv")
#file_path = r"C:\Users\kranz-michael\projects\frictionless-jcoin\hubs\data\tests\test1.tsv"
file_path = (r"C:\Users\kranz-michael\projects\rcg-bsd-gitlab\jcoin-maarc\hubs\general\quarterly-report-2021q4\university-of-kentucky\demographic-university-of-kentucky-client.tsv","")




runner = CliRunner()
result = runner.invoke(validate,["--schema-path",schema_path,"--file-path",file_path])
print(result.output)
# from frictionless import Schema,Resource
# schema = Schema(schema_path)
# resource = Resource([f for f in file_path if f] ,schema=schema)
# report = create_resource_validation_report(resource)
# report = validate_resource(resource)
# file_path_exp = parse("$..resource.path[*]")
# file_paths = [x.value for x in file_path_exp.find(report)]

# file_names = " and ".join([os.path.split(f)[-1] for f in file_paths])
# errors = [task['errors'][0] for task in report['tasks'] if not report['valid']]
