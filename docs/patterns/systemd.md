---
title: "Running RQ Workers under systemd"
layout: patterns
---

## Running RQ Workers Under systemd

[systemd][1] is process manager that's built into many popular Linux distributions.

To run multiple workers under systemd, you'll first need to create a unit file.
We can name this file `rqworker@.service`, put this file in `/etc/systemd/system`
on Ubuntu. Where you put this file may differ by what OS you run.

{% highlight ini %}
[Unit]
Description=RQ Worker Number %i
After=network.target

[Service]
Type=simple
WorkingDirectory=/path/to/working_directory
Environment=LANG=en_US.UTF-8
Environment=LC_ALL=en_US.UTF-8
Environment=LC_LANG=en_US.UTF-8
ExecStart=/path/to/rq worker -c config.py
ExecReload=/bin/kill -s HUP $MAINPID
ExecStop=/bin/kill -s TERM $MAINPID
PrivateTmp=true
Restart=always

[Install]
WantedBy=multi-user.target
{% endhighlight %}

If your unit file is properly installed, you should be able to start workers by
invoking `systemctl start rqworker@1.service`, `systemctl start rqworker@2.service`.
You can also reload all the workers by invoking `systemctl reload rqworker@*`.

You can read more about systemd and unit files [here](https://www.digitalocean.com/community/tutorials/understanding-systemd-units-and-unit-files).
