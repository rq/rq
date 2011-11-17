---
title: "RQ: Simple job queues for Python"
layout: default
---

<div class="warning">
    <img style="float: right; margin-right: -60px; margin-top: -38px; height: 100px;" src="http://a.dryicons.com/images/icon_sets/colorful_stickers_icons_set/png/256x256/warning.png" />
    <strong>Be warned!</strong>
    <p>The stuff below does not exist yet!  I merely use this page to daydream about features and to do some document-driven design.</p>
</div>


## Starting workers

To start crunching work, simply start a worker from the root of your project
directory:

{% highlight console %}
$ rqworker high,normal,low
Starting worker...
Loading environment...
Getting ready to start working...
Registering worker...
Listening on queues high, normal, low.
Ready to receive work...
{% endhighlight %}

Each worker will process a single job at a time.  Within a worker, there is no
concurrent processing going on.  If you want to perform jobs concurrently,
simply start more workers.
