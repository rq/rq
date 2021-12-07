---
title: "Putting RQ under supervisor"
layout: patterns
---

## Putting RQ under supervisor

[Supervisor][1] is a popular tool for managing long-running processes in
production environments.  It can automatically restart any crashed processes,
and you gain a single dashboard for all of the running processes that make up
your product.

RQ can be used in combination with supervisor easily.  You'd typically want to
use the following supervisor settings:

```
[program:myworker]
; Point the command to the specific rq command you want to run.
; If you use virtualenv, be sure to point it to
; /path/to/virtualenv/bin/rq
; Also, you probably want to include a settings module to configure this
; worker.  For more info on that, see http://python-rq.org/docs/workers/
command=/path/to/rq worker -c mysettings high default low
; process_num is required if you specify >1 numprocs
process_name=%(program_name)s-%(process_num)s

; If you want to run more than one worker instance, increase this
numprocs=1

; This is the directory from which RQ is ran. Be sure to point this to the
; directory where your source code is importable from
directory=/path/to

; RQ requires the TERM signal to perform a warm shutdown. If RQ does not die
; within 10 seconds, supervisor will forcefully kill it
stopsignal=TERM

; These are up to you
autostart=true
autorestart=true
```

### Conda environments

[Conda][2] virtualenvs can be used for RQ jobs which require non-Python
dependencies. You can use a similar approach as with regular virtualenvs.

```
[program:myworker]
; Point the command to the specific rq command you want to run.
; For conda virtual environments, install RQ into your env.
; Also, you probably want to include a settings module to configure this
; worker.  For more info on that, see http://python-rq.org/docs/workers/
environment=PATH='/opt/conda/envs/myenv/bin'
command=/opt/conda/envs/myenv/bin/rq worker -c mysettings high default low
; process_num is required if you specify >1 numprocs
process_name=%(program_name)s-%(process_num)s

; If you want to run more than one worker instance, increase this
numprocs=1

; This is the directory from which RQ is ran. Be sure to point this to the
; directory where your source code is importable from
directory=/path/to

; RQ requires the TERM signal to perform a warm shutdown. If RQ does not die
; within 10 seconds, supervisor will forcefully kill it
stopsignal=TERM

; These are up to you
autostart=true
autorestart=true
```

[1]: http://supervisord.org/
[2]: https://conda.io/docs/
