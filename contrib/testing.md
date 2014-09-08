---
title: "Testing"
layout: contrib
---

### Testing RQ locally

To run tests locally;

```
./run_tests
```

### Testing RQ with Vagrant

This requires several libraries, and you can keep these self contained by using [Vagrant](https://www.vagrantup.com/). 

To create a working vagrant environment, use the following;

```
vagrant init ubuntu/trusty64
vagrant up
vagrant ssh -- "sudo apt-get -y install redis-server python-dev python-pip"
vagrant ssh -- "sudo pip install --no-input redis hiredis mock"
vagrant ssh -- "(cd /vagrant; ./run_tests)"
```
