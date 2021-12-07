---
title: "RQ: Using with Django"
layout: patterns
---

## Using RQ with Django

The simplest way of using RQ with Django is to use
[django-rq](https://github.com/ui/django-rq).  Follow the instructions in the
README.

### Manually

In order to use RQ together with Django, you have to start the worker in
a "Django context".  Possibly, you have to write a custom Django management
command to do so.  In many cases, however, setting the `DJANGO_SETTINGS_MODULE`
environmental variable will already do the trick.

If `settings.py` is your Django settings file (as it is by default), use this:

```console
$ DJANGO_SETTINGS_MODULE=settings rq worker high default low
```
