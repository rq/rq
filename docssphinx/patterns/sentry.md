# Sentry

## Sending Exceptions to Sentry

[Sentry](https://sentry.io) is a popular exception gathering
service. RQ allows you to very easily send job exceptions to Sentry. To
do this, you’ll need to have
[sentry-sdk](https://pypi.org/project/sentry-sdk/) installed.

There are a few ways to start sending job exceptions to Sentry.

### Configuring Sentry Through CLI

Simply invoke the `rqworker` script using the `--sentry-dsn`
argument.

```console
$ rq worker --sentry-dsn https://my-dsn@sentry.io/123
```

### Configuring Sentry Through a Config File

Declare `SENTRY_DSN` in RQ’s config file like this:

```python
SENTRY_DSN = 'https://my-dsn@sentry.io/123'
```

And run RQ’s worker with your config file:

```console
$ rq worker -c my_settings
```

Visit {ref}`this page <usingaconfigfile>` to
read more about running RQ using a config file.

### Configuring Sentry Through Environment Variable

Simple set `RQ_SENTRY_DSN` in your environment variable and RQ will
automatically start Sentry integration for you.

```console
$ RQ_SENTRY_DSN="https://my-dsn@sentry.io/123" rq worker
```
