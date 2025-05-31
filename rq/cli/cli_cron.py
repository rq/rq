import logging
import logging.config
import sys

import click

from rq.cli.cli import main
from rq.cli.helpers import (
    pass_cli_config,
    read_config_file,
    # setup_loghandlers_from_args is not used when only --logging-level is present
)
from rq.cron import CronScheduler


@main.command()
@click.option(
    '--logging-level',
    '-l',
    type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], case_sensitive=False),
    default='INFO',
    show_default=True,  # Explicitly show the default in help text
    help='Set logging level.',
)
@click.argument('config_path')
@pass_cli_config
def cron(
    cli_config,
    logging_level,
    # verbose and quiet parameters are removed
    config_path,
    **options,
):
    """Starts the RQ cron scheduler.

    Requires a configuration file or module path defining the cron jobs.
    Logging level is controlled by the --logging-level option.
    """
    settings = read_config_file(cli_config.config) if cli_config.config else {}
    dict_config = settings.get('DICT_CONFIG')

    # Apply custom logging configuration if provided
    if dict_config:
        logging.config.dictConfig(dict_config)
        logging.getLogger('rq.cron').info('Logging configured via DICT_CONFIG setting.')

    try:
        cron = CronScheduler(connection=cli_config.connection, logging_level=logging_level)
        cron.load_config_from_file(config_path)
        cron.start()
    except KeyboardInterrupt:
        click.echo('\nShutting down cron scheduler...')
        sys.exit(0)
