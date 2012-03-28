---
title: "RQ: Using with Django"
layout: patterns
---

## Using RQ with Django

In order to use RQ together with Django, you have to start the worker in
a "Django context".  Possibly, you have to write a custom Django management
command to do so.  In many cases, however, setting the `DJANGO_SETTINGS_MODULE`
environmental variable will already do the trick.

If settings.py is your Django settings file (as it is by default), use this:

{% highlight console %}
$ DJANGO_SETTINGS_MODULE=settings rqworker high default low
{% endhighlight %}
