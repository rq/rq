from datetime import timedelta


def strip_microseconds(date):
    return date - timedelta(microseconds=date.microsecond)

