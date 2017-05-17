import click

from sentinelhub import download

@click.command()
@click.argument('tileID')
def hello(tileid):
    click.echo("Sentinel Hub download util")
    click.echo(download(tileid))
