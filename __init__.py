import click

from msc_pygeoapi.process.weather.test import test_execute


@click.group()
def weather():
    pass


weather.add_command(test_execute)