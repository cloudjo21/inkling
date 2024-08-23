import click

from tunip import file_utils, path_utils
from tunip.service_config import get_service_config

from inkling import LOGGER


@click.command()
@click.option(
    "--source",
    type=click.STRING,
    required=True,
    help="Blob path for source directory"
)
@click.option(
    "--target",
    type=click.STRING,
    required=True,
    help="Blob path for target directory"
)
def main(source, target):
    LOGGER.info(f"source: {source} -> target: {target}")
    service_config = get_service_config()
    file_service = file_utils.services(service_config)

    file_service.mkdirs(target)
    file_service.copy_files(source, target)

if __name__ == "__main__":
    main()
