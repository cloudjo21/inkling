import logging

from tunip.logger import init_logging_handler


LOGGER = init_logging_handler(name="inkling", level=logging.INFO)
