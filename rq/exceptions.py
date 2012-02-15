class NoSuchJobError(Exception):
    pass


class InvalidJobOperationError(Exception):
    pass


class NoQueueError(Exception):
    pass


class UnpickleError(Exception):
    def __init__(self, message, raw_data):
        super(UnpickleError, self).__init__(message)
        self.raw_data = raw_data
