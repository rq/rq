from rq import cron
from tests.fixtures import div_by_zero, do_nothing, say_hello


# Define additional test function
def calculate_value(a, b):
    """Function that performs a calculation."""
    return a + b


# Register jobs with various configurations

# 1. Basic job that runs every minute
cron.register(say_hello, queue_name='default', interval=60)

# 2. Basic job with name parameter
cron.register(say_hello, queue_name='default', kwargs={'name': 'RQ Cron'}, interval=120)

# 3. Job that will fail with division by zero
cron.register(
    div_by_zero,
    queue_name='default',
    args=(10,),
    interval=180,
)

# 4. Job with short interval
cron.register(do_nothing, queue_name='default', interval=30)
