"""CLI for JDC utilities"""

import click
from submission import build_resource, create_resource_validation_report
from transforms import read_df, run_transformfile
import dataforge_ids as ids

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
    for file_name in filepath:
        df = read_df(file_name)
        df_new = ids.replace_ids(
            df, id_file=id_file, map_file=map_file, map_url=map_url, column=column
        )
        df_new.to_csv(f"jdc_files/{hub}_{file_name.split('/')[-1]}")


@click.command()
@click.option("--transform-file", help="Path to the given transform file")
@click.option("--file-path", help="Path to the given file")
def transform(transform_file, file_path):
    df = read_df(file_path)
    run_transformfile(df, transform_file)
    transform_dir = os.path.join("tmp", "transformed-data")
    os.makedirs(transform_dir, exist_ok=True)
    file_name = os.path.split(file_path)[-1]
    file_path_to_save = os.path.join(transform_dir, file_name)
    df.to_csv(file_path_to_save, sep="\t")
    click.echo(f"Transformed file saved to {file_path_to_save}")


@click.command()
@click.option(
    "--schema", help="Frictionless table schema JSON or YAML file path"
)  # make this either a path or a option of baseline/followup
@click.option(
    "--file-path",
    help="Path to a file. Can specify multiple files if fields span multiple files",
    multiple=True,
)
def validate(schema, file_path):
    resource = build_resource(schema_path, file_path)
    report = create_resource_validation_report(resource)
    click.echo(report)


cli.add_command(replace_ids, name="replace-ids")
cli.add_command(transform, name="transform")
cli.add_command(validate, name="validate")


if __name__ == "__main__":
    cli()
