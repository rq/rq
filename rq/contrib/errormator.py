from errormator_client.ext.general import gather_data

def register_errormator(client, worker):
    """Register exception handler that will relay exceptions to Errormator."""
    
    def process_exception(job, *exc_info):
        fake_environ = {'PATH_INFO':'/rq/' + job.func_name,
                        'errormator.post_vars':{'job_args':job.args,
                                                'job_kwargs': job.kwargs},
                        'errormator.force_send':1,
                        'errormator.report_local_vars':1}
        gather_data(client, fake_environ, spawn_thread=False)
    worker.push_exc_handler(process_exception,)