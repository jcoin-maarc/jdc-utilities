"""CLI for JDC utilities"""

import click
from jdc_utils.submission import build_resource, create_resource_validation_report
from jdc_utils.transforms import read_df, run_transformfile
import jdc_utils.dataforge_ids as ids
from frictionless import Schema, Resource
from jdc_utils.utils import copy_file
import os
import pandas as pd

# overall CLI
@click.group()
def cli():
    """CLI for JDC utilities"""
    pass


@click.command()
@click.option(
    "--file-path",
    help="Path to a file. Can specify multiple files if need to replace ids across multiple files",
    multiple=True,
    required=True,
)
@click.option(
    "--id-file",
    help="path to csv where the id mappings are stored -- this will be generated if file does not exist",
    required=True,
)
@click.option(
    "--map-file",
    help="Path to where the git repository supporting the versioning",
    required=True,
)
@click.option("--map-url", help="Git bare repo set up", required=True)
@click.option("--column", help="Name of column across files", default=None)
def replace_ids(file_path, id_file, map_file):
    new_dir = os.path.join("jdc-data", "replaced-ids-but-not-validated")
    for file_name in filepath:
        df = read_df(file_name)
        df_new = ids.replace_ids(
            df, id_file=id_file, map_file=map_file, map_url=map_url, column=column
        )
        new_file_dir = os.path.join(new_dir, file_name.split("/")[-1])
        df_new.to_csv(new_file_dir)


@click.command()
@click.option("--transform-file", help="Path to the given transform file")
@click.option("--file-path", help="Path to the given file")
def transform(transform_file, file_path):

    # read in and run transforms -- right now currently using pandas -- may want to migrate to petl
    # for consistency with validation as it uses petl to read in and type conversions may be different.
    # alternatively, we could use the pandas plugin for frictionless but it is experimental.
    df = read_df(file_path)
    run_transformfile(df, transform_file)

    # make transform dir
    transform_dir = os.path.join("jdc-data", "transformed-but-not-validated")
    os.makedirs(transform_dir, exist_ok=True)

    # save file
    file_name = os.path.split(file_path)[-1]
    file_path_to_save = os.path.join(transform_dir, file_name)
    df.to_csv(file_path_to_save)
    click.echo(f"Transformed file saved to {file_path_to_save}")


@click.command()
@click.option(
    "--schema-path", help="Frictionless table schema JSON or YAML file path"
)  # make this either a path or a option of baseline/followup
@click.option(
    "--file-path",
    help="Path to a file. Can specify multiple files if fields span multiple files",
    multiple=True,
)
@click.option(
    "--file-type",
    help="Type of file(s). Currently either baseline or time-points"
)
def validate(schema_path, file_path,file_type):
    if file_type:
        if file_type=='baseline':
            #file_path needs to be iterable as there is the ability to have multiple resources
            schema_path = r"C:\Users\kranz-michael\projects\frictionless-jcoin\hubs\metadata\table_schemas\table-schema-baseline.json"
    file_path = [f for f in file_path if f]  # get rid of ""
    resource = build_resource(schema_path, file_path)
    report = create_resource_validation_report(resource)

    validated_dir = os.path.join("jdc-data", "validated")
    os.makedirs(validated_dir, exist_ok=True)

    if report["is_valid"]:
        click.echo(
            f"Congrats! Your file(s) -- {report['file_names']} ---  passed validation!\n"
            f"Now saving to {validated_dir}"
        )
        click.echo("If you're happy with them, you can proceed with submission.")
        # save to file
        for f in file_path:
            validated_file_path = os.path.join(validated_dir, os.path.split(f)[-1])
            copy_file(f, validated_file_path)
    else:
        click.echo(
            "\n\n"
            f"{report['file_names']}: invalid.\n"
            f"Take a look below at the error report table below to correct.\n"
            f"We'll also save your errors to the {validated_dir}\errors.tsv file\n"
            "----------------------------------------------------------------------"
            "----------------------------------------------------------------------"
        )
        report['errors_df'].replace('"','',inplace=True)
        click.echo(report['errors_df'][['error-category','error-message',]].to_string(index=False))
        # append error.tsv
        report['errors_df'].to_csv(os.path.join(validated_dir,"errors.tsv"),sep="\t")

cli.add_command(replace_ids, name="replace-ids")
cli.add_command(transform, name="transform")
cli.add_command(validate, name="validate")


if __name__ == "__main__":
    cli()
