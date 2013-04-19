from .utils import parse_func


class Registry(object):
    """
    A central registry that holds a map of callable name to job ids. Each
    registry is persisted as a Redis set from which we can very cheaply add,
    remove and check for a job's existence.
    """

    def __init__(self, name, connection):
        self.connection = connection
        self.name = name
        self.key = 'rq:registry:%s' % name

    def get_key_from_job(self, job):
        """
        Returns a registry key under which a job is registered at. A registry
        key should be of the following format:
            rq:registry:registry_name:callable_name
        """
        if job.instance:
            return '%s:%s.%s.%s' % (
                self.key,
                job.instance.__module__,
                job.instance.__class__.__name__,
                job.func_name
            )
        return '%s:%s' % (self.key, job.func_name)

    def get_key_from_callable(self, f):
        """
        Returns a registry key for a callable, functions look like:
            rq:registry:registry_name:callable_name

        Methods look like:
            rq:registry:registry_name:path.to.Class.method
        """
        instance, func_name = parse_func(f)
        if instance:
            return '%s:%s.%s.%s' % (
                self.key,
                instance.__module__,
                instance.__class__.__name__,
                func_name
            )
        return '%s:%s' % (self.key, func_name)

    def register(self, job):
        self.connection.sadd(self.get_key_from_job(job), job.id)

    def unregister(self, job):
        self.connection.srem(self.get_key_from_job(job), job.id)

    def is_registered(self, f):
        """
        Returns True if one or more jobs of the containing callable is in the
        registry.
        """
        return self.connection.exists(self.get_key_from_callable(f))
