from tests import find_empty_redis_database

#: The contract's own Redis database: one that was empty when it was picked, emptied around every case.
CONNECTION = find_empty_redis_database()
