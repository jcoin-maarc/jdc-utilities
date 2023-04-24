"""CLI for JDC utilities"""
import os
from pathlib import Path

import click
import confuse
from jdc_utils.submission import CoreMeasures
from jdc_utils.transforms import read_df, run_transformfile
from jdc_utils.transforms.deidentify import init_version_history_all

from jdc_utils import config


# overall CLI
@click.group()
def cli():
    """CLI for JDC utilities"""
    pass


@click.command("run", context_settings={"default_map": config.get()})
@click.option("--history-path")
@click.option("--filepath", default="tmp/local")
@click.option("--id-file", default="data_mgmt/id_store/jdc_person_ids.txt")
@click.option("--id-column", default=None)
@click.option("--date-columns", default=None, multiple=True)
@click.option("--outdir", default="tmp/core-measures")
@click.option("--validate-only", is_flag=True, default=False)
@click.option("--deidentify-only", is_flag=True, default=False)
def run(
    history_path,
    filepath,
    id_file,
    id_column,
    date_columns,
    outdir,
    validate_only,
    deidentify_only,
):
    # CHECK: if running deidentify need these params
    if not validate_only or deidentify_only:
        assert history_path
        assert filepath
        assert id_file
        assert id_column
        assert date_columns
        assert outdir
    elif validate_only:
        assert filepath
        assert outdir

    date_columns = list(date_columns)

    core_measures = CoreMeasures(
        filepath=filepath,
        id_file=id_file,
        id_column=id_column,
        date_columns=date_columns,
        outdir=outdir,
        history_path=history_path,
    )
    # 1. running entire pipeline
    if not validate_only and not deidentify_only:
        core_measures.deidentify()
        core_measures.write()
    # 2. deidentified but not in package
    elif deidentify_only:
        Path("tmp/deidentified").mkdir(exist_ok=True, parents=True)
        core_measures.deidentify()
        for resource in core_measures.package.resources:
            resource.write(f"tmp/deidentified/{resource.name}.csv")
    # 3. already deidentified -- just need to package and validate
    elif validate_only:
        core_measures.write()


id_file_prompt = """
Enter the path to your generated id file (This may be something like ids/submitter_ids.txt or ids/jdc_person_ids.txt
and will be generated by the MAARC to ensure no overlap between hubs):
"""
id_column_prompt = """
Enter the name of your participant id column below: (This may be PID, record_id, etc
that your hubs use for individual participants)
"""

file_path_prompt = """
Enter the directory that contains (or will contain) the core measure data files.
Leave blank (press return) for the default tmp/local. And then copy files here
(should contain one file for each type of schema/data dictionary).
"""

history_prompt = """
Enter your desired path to version control history (ie the id and date shift mappings)
"""

date_columns_prompt = """
Enter all date column names in any of your core measure files such as visit_dt.
If multiple date columns, separate with a space. If you're submitting
a package without a date column (eg a baseline file), you can either leave blank
or avoid work later and add future dateset (eg timepoints) date columns.
"""


@click.command(
    "init", help="Initiates both a version control directory and a config file."
)
@click.option("--history-path", prompt=history_prompt)
@click.option(
    "--filepath",
    default="tmp/local",
    prompt=file_path_prompt,
    help="Path to the to-be processed (e.g., deidientifed, transferred to core measure package. If it doesn't exist, will create and you can copy your files here",
)
@click.option(
    "--id-file", prompt=id_file_prompt, default="data_mgmt/id_store/jdc_person_ids.txt"
)
@click.option("--id-column", prompt=id_column_prompt)
@click.option("--date-columns", prompt=date_columns_prompt)
def init(history_path, filepath, id_file, id_column, date_columns):
    # create version control history
    if Path(history_path).exists():
        click.echo(f"{history_path} already exists so skipping")
    else:
        init_version_history_all(history_path)

    # make directories
    Path(filepath).mkdir(exist_ok=True, parents=True)
    Path(id_file).parent.mkdir(exist_ok=True, parents=True)

    with open("config.yaml", "w", newline="") as f:
        # NOTE: posix path needed for frictionless paths
        f.write(f"history_path: {(Path(history_path).resolve().as_posix())}")
        f.write(os.linesep)
        f.write(f"filepath: {Path(filepath).resolve().as_posix()}")
        f.write(os.linesep)
        f.write(f"outdir: {Path('tmp/core-measures/').resolve().as_posix()}")
        f.write(os.linesep)
        f.write(f"id_file: {Path(id_file).resolve().as_posix()}")
        f.write(os.linesep)
        f.write(f"id_column: {id_column}")
        f.write(os.linesep)
        date_list = "\n - ".join(date_columns.split(" "))
        f.write(f"date_columns:\n - {date_list}")
        f.write(os.linesep)

    click.echo(os.linesep)
    click.echo("config.yaml created!")


@click.command(
    """
    This input file transforms a given dataset with a variety of functions or any function
    available within pandas that has an "inplace" argument.

    Most commonly, this will entail renaming variables and values to match the data model
    schema in order to pass validation (see the jdc-utils validate function)

    """
)
@click.option(
    "--file-path",
    "file_paths",
    help="Path to the given dataset file. Can specify multiple files if same transform file used across multiple data files.",
    multiple=True,
    required=True,
)
@click.option("--transform-file", help="Path to the given transform file")
def transform(transform_file, file_paths):
    # read in and run transforms -- right now currently using pandas -- may want to migrate to petl
    # for consistency with validation as it uses petl to read in and type conversions may be different.
    # alternatively, we could use the pandas plugin for frictionless but it is experimental.
    # click.echo("STARTING")
    for file_path in file_paths:
        # glob.glob allows support for both wildcards (*) and actual file paths
        file_path_with_glob_regexs = glob.glob(
            file_path
        )  # if not a regex, will just return the filepath within list

        print(f"Applying {transform_file} to:")
        print(",".join(file_path_with_glob_regexs))

        for file_path_glob in file_path_with_glob_regexs:
            sourcedf = read_df(file_path_glob)
            targetdf = run_transformfile(df, transform_file)
            Path("tmp/jdc").mkdir(exist_ok=True, parents=True)
            targetpath = f"tmp/jdc/{Path(file_path).stem}-transformed.csv"
            targetdf.to_csv(targetpath, index=False)
            click.echo(f"Transformed file saved to {targetdf}")


# deidentification commands
cli.add_command(run, name="run")
cli.add_command(init, name="init")
# pipeline
cli.add_command(transform, name="transform")

if __name__ == "__main__":
    cli()
