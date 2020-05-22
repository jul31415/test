import click

from msc_pygeoapi.process.weather.test import cli


@click.group()
def weather():
    pass


weather.add_command(cli)