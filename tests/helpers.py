import times


def strip_milliseconds(date):
    return times.to_universal(times.format(date, 'UTC'))

