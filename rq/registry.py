from .utils import get_func_name


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
        return '%s:%s' % (self.key, job.func_name)

    def get_key_from_callable(self, f):
        """
        Returns a registry key for a callable, it should look something like:
            rq:registry:registry_name:callable_name
        """
        return '%s:%s' % (self.key, get_func_name(f))        

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