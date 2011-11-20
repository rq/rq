---
title: "RQ: Simple job queues for Python"
layout: default
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


## Showing active workers

To see what workers are currently active:

{% highlight console %}
$ rqinfo --workers
worker          queues            jobs   state
--------------- ----------------- ------ --------
mickey.27939    high,normal,low   34     busy
mickey.8239     high,normal,low   120    busy
turkish.32682   high              200    idle
bricktop.2682   weekly            1      starting
foobar.2658     default           1      idle
4 workers listening on 4 queues
{% endhighlight %}

The states (_starting_, _idle_, _busy_, or _dead_) indicate the phase of the
worker life-cycle (see [workers][w]).

[w]: {{site.baseurl}}workers/

In the future, _paused_ would be a useful state, too.


{% highlight console %}
$ rqinfo --workers --verbose
worker          queues            jobs   since      state
--------------- ----------------- ------ ---------- --------
mickey.27939    high,normal,low   34     4d 11h 6m  busy
mickey.8239     high,normal,low   120    21d 2h 4m  busy
turkish.32682   high              200    1h 7m 3s   idle
bricktop.2682   weekly            1      1m 7s      starting
foobar.2658     default           1      1m 7s      idle
4 workers listening on 4 queues
{% endhighlight %}


