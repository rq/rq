class NoSuchJobError(Exception):
    pass


class InvalidJobOperationError(Exception):
    pass


class NoQueueError(Exception):
    pass


class UnpickleError(Exception):
    def __init__(self, message, raw_data, inner_exception):
        super(UnpickleError, self).__init__(message, inner_exception)
        self.raw_data = raw_data
