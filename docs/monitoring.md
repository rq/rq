---
title: "RQ: Monitoring"
layout: docs
---

<div class="warning">
    <img style="float: right; margin-right: -60px; margin-top: -38px; height: 100px;" src="http://a.dryicons.com/images/icon_sets/colorful_stickers_icons_set/png/256x256/warning.png" />
    <strong>Be warned!</strong>
    <p>The stuff on this page does not exist yet.  I merely use this page to do some document-driven design.</p>
</div>

Monitoring is where RQ shines.


## Showing queues

To see what queues exist (and how many jobs there are to process them):

{% highlight console %}
$ rqinfo --queues
high       20
low        12
normal     8
default    0
4 queues, 45 jobs total
{% endhighlight %}


{% highlight console %}
$ rqinfo --queues --graph
high       |██████████████████████████ 20
low        |██████████████ 12
normal     |█████████ 8
default    | 0
4 queues, 45 jobs total
{% endhighlight %}


You can also query for a subset of queues, if you're looking for specific ones:

{% highlight console %}
$ rqinfo --queues --graph high normal
high       |██████████████████████████ 20
normal     |█████████ 8
2 queues, 28 jobs total
{% endhighlight %}


### Interval polling

By default, `rqinfo` prints the stats and exists.  You can however tell it to
keep polling for the same data periodically, by using the `--interval` flag.
`rqinfo` then puts itself into an endless polling loop, which is especially
attractive when used together with the `--graph` option.

{% highlight console %}
$ rqinfo --queues --graph --interval 1
{% endhighlight %}

`rqinfo` will now update the screen with the new info every second.  If you
feel the need, you can specify fractions of seconds (although this might incur
some extra polling load):

{% highlight console %}
$ rqinfo --queues --graph --interval 0.5
{% endhighlight %}


## Showing active workers

To see what workers are currently active, and what queues they operate on:

{% highlight console %}
$ rqinfo --workers
Mickey.26421 ●: high, normal, default
Bricktop.25458 ●: high, normal, low
Turkish.25812 ●: high, normal
3 workers, 4 queues
{% endhighlight %}

To see the same data, but organised by queue, use the `-Q` (or `--by-queue`) flag:

{% highlight console %}
$ bin/rqinfo --workers -Q
default: Mickey.26421 (●)
high:    Bricktop.25458 (●), Mickey.26421 (●), Turkish.25812 (●)
low:     Bricktop.25458 (●)
normal:  Bricktop.25458 (●), Mickey.26421 (●), Turkish.25812 (●)
3 workers, 4 queues
{% endhighlight %}

The dots behind the worker names indicate the current state of the worker.
A open green dot means idle and a filled red dot means busy.


### Interval polling

Just like with `rqinfo --queues`, you can use the `--interval` option to keep
polling for the worker stats at a regular interval.

