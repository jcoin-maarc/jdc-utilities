"""CLI for JDC utilities"""

import click
from jdc_utils.submission import build_resource, create_resource_validation_report
from jdc_utils.transforms import read_df, run_transformfile
import jdc_utils.dataforge_ids as ids
from frictionless import Schema, Resource
from jdc_utils.utils import copy_file

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
def validate(schema_path, file_path):
    file_path = [f for f in file_path if f]  # get rid of ""
    resource = build_resource(schema_path, file_path)
    report = create_resource_validation_report(resource)

    validated_dir = os.path.join("jdc-data", "validated")
    os.makedirs(validated_dir, exists=True)

    if report["valid"]:
        click.echo(
            f"Congrats! Your file(s) passed validation! Now saving to {save_dir}"
        )
        click.echo("If you're happy with them, you can proceed with submission.")
        # save to file
        validated_file_path = os.path.join(validated_dir, os.path.split(file_path)[-1])
        copy_file(file_path, validated_file_path)
    else:
        click.echo(
            f"Invalid files. Take a look below at the error report below to correct."
        )
        click.echo(f"We'll also save your errors to the {save_dir}/errors.tsv file")

        report_table = pd.DataFrame(
            report.flatten(["code", "rowPosition", "message", "description"]),
            columns=[
                "error-category",
                "row-number",
                "error-message",
                "general-error-description",
            ],
        )
        click.echo(report_table.to_csv(sep="\t"))
        # append error.tsv
        report_table.to_csv(os.path.join(validated_dir,"errors.tsv"),sep="\t")

cli.add_command(replace_ids, name="replace-ids")
cli.add_command(transform, name="transform")
cli.add_command(validate, name="validate")


if __name__ == "__main__":
    cli()
