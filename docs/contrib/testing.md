---
title: "Testing"
layout: contrib
---

### Testing RQ locally

To run tests locally you can use `tox`, which will run the tests with all supported Python versions (3.6 - 3.11)

```
tox
```

Bear in mind that you need to have all those versions installed in your local environment for that to work.

### Testing with Pytest directly

For a faster and simpler testing alternative you can just run `pytest` directlyL

```sh
pytest .
```

It should automatically pickup the `tests` directory and run the test suite.
Bear in mind that some tests will be skipped in a local environment - make sure to look at what tests are being skipped.


### Skipped Tests

Apart from skipped tests related to interpreter (eg `PyPy`) or operational systems, slow tests are also skipped by default, but are ran in the GitHub CI/CD workflow.
To include slow tests in your local environment, use the `RUN_SLOW_TESTS_TOO=1` environment variable:

```sh
RUN_SLOW_TESTS_TOO=1 pytest .
```

If you want to analyze the coverage reports, you can use the `--cov` argument to `pytest`:

```sh
RUN_SLOW_TESTS_TOO=1 pytest --cov=./ --cov-report={{report_format}} --durations=5
```

Where you replace thte `report_format` fot the desired formatting ()`term` / `html` / `xml`).

### Using Vagrant

If you rather use Vagrant, see [these instructions][v].

[v]: {{site.baseurl}}contrib/vagrant/
