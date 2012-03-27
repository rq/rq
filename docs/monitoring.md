---
title: "RQ: Monitoring"
layout: docs
---

Monitoring is where RQ shines.

To see what queues exist and what workers are active, just type `rqinfo`:

{% highlight console %}
$ rqinfo
high       |██████████████████████████ 20
low        |██████████████ 12
default    |█████████ 8
3 queues, 45 jobs total

Bricktop.19233 idle: low
Bricktop.19232 idle: high, default, low
Bricktop.18349 idle: default
3 workers, 3 queues
{% endhighlight %}


## Querying by queue names

You can also query for a subset of queues, if you're looking for specific ones:

{% highlight console %}
$ rqinfo high default
high       |██████████████████████████ 20
default    |█████████ 8
2 queues, 28 jobs total

Bricktop.19232 idle: high, default
Bricktop.18349 idle: default
2 workers, 2 queues
{% endhighlight %}


## Organising workers by queue

By default, `rqinfo` prints the workers that are currently active, and the
queues that they are listening on, like this:

{% highlight console %}
$ rqinfo
...

Mickey.26421 idle: high, default
Bricktop.25458 busy: high, default, low
Turkish.25812 busy: high, default
3 workers, 3 queues
{% endhighlight %}

To see the same data, but organised by queue, use the `-R` (or `--by-queue`)
flag:

{% highlight console %}
$ rqinfo -R
...

default: Mickey.26421 (idle)
high:    Bricktop.25458 (busy), Mickey.26421 (idle), Turkish.25812 (busy)
low:     Bricktop.25458 (busy)
default: Bricktop.25458 (busy), Mickey.26421 (idle), Turkish.25812 (busy)
3 workers, 4 queues
{% endhighlight %}


## Interval polling

By default, if `rqinfo` is ran in a terminal session, it will print stats and
autorefresh them every 2.5 seconds.  You can specify a different poll interval,
by using the `--interval` flag.

{% highlight console %}
$ rqinfo --interval 1
{% endhighlight %}

`rqinfo` will now update the screen every second.  You may specify a float
value to indicate fractions of seconds.  Be aware that low interval values will
increase the load on Redis, of course.

{% highlight console %}
$ rqinfo --interval 0.5
{% endhighlight %}

Note that if stdout is redirected (i.e. stdout is not a TTY), `rqinfo` will
only output the stats once, and won't loop.
