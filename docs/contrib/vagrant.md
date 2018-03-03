---
title: "Using Vagrant"
layout: contrib
---

If you don't feel like installing dependencies on your main development
machine, you can use [Vagrant](https://www.vagrantup.com/).  Here's how you run
your tests and build the documentation on Vagrant.


### Running tests in Vagrant

To create a working Vagrant environment, use the following;

```
vagrant init ubuntu/trusty64
vagrant up
vagrant ssh -- "sudo apt-get -y install redis-server python-dev python-pip"
vagrant ssh -- "sudo pip install --no-input redis hiredis mock"
vagrant ssh -- "(cd /vagrant; ./run_tests)"
```


### Running docs on Vagrant

```
vagrant init ubuntu/trusty64
vagrant up
vagrant ssh -- "sudo apt-get -y install ruby-dev nodejs"
vagrant ssh -- "sudo gem install jekyll"
vagrant ssh -- "(cd /vagrant; jekyll serve)"
```

You'll also need to add a port forward entry to your `Vagrantfile`;

```
config.vm.network "forwarded_port", guest: 4000, host: 4001
```

Then you can access the docs using;

```
http://127.0.0.1:4001
```

You also may need to forcibly kill Jekyll if you ctrl+c;

```
vagrant ssh -- "sudo killall -9 jekyll"
```
