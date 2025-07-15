## Notes on Performance

**TL;DR — run `Worker` or `SpawnWorker` in production.**

In a simple hello world microbenchmark, `SimpleWorker` processed 1,000 jobs in just 1.02 seconds vs. 6.64 s with the default `Worker`), roughly a 6x speedup.

`SimpleWorker` is faster because it skips `fork()` or `spawn()` and runs jobs in process. `Worker` and `SpawnWorker` run each job in a separate process, acting as a sandbox that isolates crashes, memory leaks and enforce hard time-outs.

Although `SimpleWorker` is faster in benchmarks, this overhead is negligible for most real world applications like sending emails, generating reports, processing images, etc. In production systems, the time spent performing jobs usually dwarfs any queueing/worker overhead.

Use `SimpleWorker` in production only if:
* Your jobs are extremely short-lived (single digit milliseconds).
* The `fork()` or `spawn()` latency is a proven bottleneck at your traffic levels.
* Your job code is 100% trusted and known to be free of resource leaks and the possibility of crashing/segfaults.


> "Lies, damned lies, and benchmarks." — Mark Twain

These numbers are illustrative only – real-world results will vary. Tested on:
* M4 MacBook Air
* Python 3.13.2
* Local Redis 7.2.7 server

### Benchmark script

```python
from redis import Redis
from rq import Queue, SimpleWorker, Worker
from fixtures import say_hello
from datetime import datetime

queue = Queue(connection=Redis())
num_jobs = 1000

# Default Worker
for i in range(num_jobs):
    queue.enqueue(say_hello)
start = datetime.now()
Worker(queue, connection=Redis()).work(burst=True)
worker_duration = datetime.now() - start

# SimpleWorker
for i in range(num_jobs):
    queue.enqueue(say_hello)
start = datetime.now()
SimpleWorker(queue, connection=Redis()).work(burst=True)
simple_worker_duration = datetime.now() - start

print(f"Processed {num_jobs} jobs with Worker in {worker_duration.total_seconds():.5f} seconds.")
print(f"Processed {num_jobs} jobs with SimpleWorker in {simple_worker_duration.total_seconds():.5f} seconds.")
