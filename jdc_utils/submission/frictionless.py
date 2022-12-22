""" 
Generate and validate data package for submission by hubs
""" 
#get schemas and validate files, creating report,writing datasets, and package metadata
import os
from jdc_utils import schema
from frictionless import Package,Resource
from frictionless import validate
from collections import abc
from dataforge.frictionless import add_missing_fields


def _make_resource_errordf(task):
    df = pd.DataFrame([{'column':c['fieldName'],'value':c['cell'],'error':c['note']}
        for c in task['errors']])
    return df.assign(resource=task['name'])

def _add_resources_missing_fields(package):
    target = transform(
        package,
        steps=[
                steps.resource_transform(
                    name=r['name'], steps=[add_missing_fields('Missing')])
            for r in resource_names
        ],
    )       
    return target

def get_package(filepath):
    """ 
    gets a package containing the paths to resources (filepath) 
    
    Paramaters
    --------------
    filepath: can be one of:
        - a path to a data file
        - path to a glob-like regular expression for multiple data files
        - can also be a package descriptor file (eg data-package.json) with resources
        (technically can also be a Package object in addition to a file path)

    Returns
    --------------
    package with schema added and added missing values (in resource metadata and actual table)
    """ 
    package = Package(filepath)
    for resource in package['resources']:
        if not resource.get('name'):
            resource['name'] = name = Path(resource['path']).stem.lower()
        elif not resource.get('schema'):
            #convert Simplenamespace to dictionary to reference schema based on filename stem
            resource['schema'] = var(schema.core_measures)[name.replace("-","")]
    
    return _add_resources_missing_fields(package)





def write_validation_report(package):

    #add fields and write text file with missing fields

    if isinstance(package,Package):
        report = validate(package)
    elif 
    with open('report.txt','w') as f:
        f.write(report.to_summary())

    with open('report.json','w') as f:
        json.dump({prop:report[prop] for prop in ['valid','version','stats','time']},f,indent=4)

    errors_df = pd.concat([_make_resource_errordf(task) for task in report['tasks']]).drop_duplicates()
    errors_df.to_csv(Path(pkg_dir)/'report_condensed.tsv',sep='\t',index=False)




