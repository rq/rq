import logging.config


def setup_loghandlers(verbose=False):
    if not logging._handlers:
        logging.config.dictConfig({
            "version": 1,
            "disable_existing_loggers": False,

            "formatters": {
                "console": {
                    "format": "%(asctime)s %(message)s",
                    "datefmt": "%H:%M:%S",
                },
            },

            "handlers": {
                "console": {
                    "level": "DEBUG",
                    #"class": "logging.StreamHandler",
                    "class": "rq.utils.ColorizingStreamHandler",
                    "formatter": "console",
                    "exclude": ["%(asctime)s"],
                },
            },

            "root": {
                "handlers": ["console"],
                "level": "DEBUG" if verbose else "INFO"
            }
        })
