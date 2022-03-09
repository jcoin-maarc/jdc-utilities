"""CLI for JDC utilities"""

import click
from submission import validate_schema_synced_resource

#overall CLI
@click.group()
def cli():
    """CLI for JDC utilities"""
    pass


@click.command()
def replace_id():
    pass


@click.command()
def transform():
    pass


@click.command()
@click.option("--schemapath",help="Frictionless table schema JSON or YAML file path")
@click.option("--filepath",help="Path to a file. Can specify multiple files if fields span multiple files",multiple=True)
def validate(schemapath,filepath):
    resource = build_resource(schemapath,filepath)
    report = create_resource_validation_report(resource)
    click.echo(report)


if __name__ == '__main__':
    cli()


