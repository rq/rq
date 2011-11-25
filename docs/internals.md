---
title: "RQ: Documentation"
layout: docs
---

The following section elaborates on some implementation details, which might be
interesting material if you're a contributor, or want to understand what
happens under the covers.


<h2 id="job-tuple">Job tuple</h2>

Internally, when enqueuing, the function and its arguments are encoded into
a job tuple of the following form:

    (func, rv_key, args, kwargs)

The tuple is then serialized using `pickle` and written to the appropriate
Redis queue.


## Failed job tuple

When jobs fail, the following data structure is put on the special `failed`
queue:

    (worker, queue, exc_info, job_tuple)

Or, put verbosely:

    (worker, queue, exc_info, (func, rv_key, args, kwargs))

Analogous to the job tuple, this tuple is serialized using pickle.
