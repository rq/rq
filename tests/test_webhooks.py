import json
from datetime import datetime, timedelta, timezone
from unittest.mock import ANY, patch

from rq.job import Job, JobStatus, Retry
from rq.queue import Queue
from rq.webhook import Webhook
from rq.worker import SimpleWorker
from tests import RQTestCase

from .fixtures import div_by_zero, fail_while_retries_remain, returns_retry, say_hello


class WebhookTestCase(RQTestCase):
    def test_init(self):
        # Defaults
        webhook = Webhook('http://example.com/hook', 'finished')
        self.assertEqual(webhook.url, 'http://example.com/hook')
        self.assertEqual(webhook.job_status, 'finished')
        self.assertEqual(webhook.method, 'GET')
        self.assertIsNone(webhook.headers)
        self.assertEqual(webhook.timeout, 10)

        # All fields set explicitly
        webhook = Webhook(
            'https://example.com/hook',
            'failed',
            method='POST',
            headers={'Authorization': 'Bearer token'},
            timeout=5,
        )
        self.assertEqual(webhook.job_status, 'failed')
        self.assertEqual(webhook.method, 'POST')
        self.assertEqual(webhook.headers, {'Authorization': 'Bearer token'})
        self.assertEqual(webhook.timeout, 5)

    def test_url_validation(self):
        with self.assertRaises(ValueError):
            Webhook('', 'finished')
        with self.assertRaises(ValueError):
            Webhook('example.com/hook', 'finished')
        with self.assertRaises(ValueError):
            Webhook('ftp://example.com', 'finished')
        with self.assertRaises(ValueError):
            Webhook('http://', 'finished')
        with self.assertRaises(ValueError):
            Webhook(None, 'finished')

    def test_job_status_validation(self):
        with self.assertRaises(ValueError):
            Webhook('http://example.com', None)
        with self.assertRaises(ValueError):
            Webhook('http://example.com', 'stopped')

    def test_method_validation(self):
        with self.assertRaises(ValueError):
            Webhook('http://example.com', 'finished', method='PUT')
        with self.assertRaises(ValueError):
            Webhook('http://example.com', 'finished', method='get')
        with self.assertRaises(ValueError):
            Webhook('http://example.com', 'finished', method=None)

    def test_headers_validation(self):
        with self.assertRaises(TypeError):
            Webhook('http://example.com', 'finished', headers=[('X-Token', 'secret')])

    def test_timeout_validation(self):
        with self.assertRaises(ValueError):
            Webhook('http://example.com', 'finished', timeout=-1)
        with self.assertRaises(ValueError):
            Webhook('http://example.com', 'finished', timeout='10')
        with self.assertRaises(ValueError):
            Webhook('http://example.com', 'finished', timeout=1.5)

    def test_to_dict(self):
        webhook = Webhook('http://example.com', 'failed', method='POST', timeout=5)
        self.assertEqual(
            webhook.to_dict(),
            {
                'url': 'http://example.com',
                'job_status': 'failed',
                'method': 'POST',
                'headers': None,
                'timeout': 5,
            },
        )

    def test_from_dict(self):
        # Round-trips a fully-specified webhook
        webhook = Webhook('http://example.com', 'finished', method='POST', headers={'X-A': 'b'}, timeout=3)
        self.assertEqual(Webhook.from_dict(webhook.to_dict()), webhook)

        # Fills defaults for missing optional fields
        webhook = Webhook.from_dict({'url': 'http://example.com', 'job_status': 'failed'})
        self.assertEqual(webhook, Webhook('http://example.com', 'failed'))

        # Re-validates its input
        with self.assertRaises(ValueError):
            Webhook.from_dict({'url': 'http://example.com', 'job_status': 'stopped'})

    def test_get_payload(self):
        job = Job.create(say_hello, connection=self.connection)
        job.enqueued_at = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        job.ended_at = datetime(2026, 1, 1, 12, 0, 5, tzinfo=timezone.utc)

        # Finished: job metadata only (the result is intentionally not included)
        payload = Webhook('http://example.com', 'finished').get_payload(job)
        self.assertEqual(
            payload,
            {
                'job_id': job.id,
                'func_name': 'tests.fixtures.say_hello',
                'status': 'finished',
                'enqueued_at': job.enqueued_at.isoformat(),
                'ended_at': job.ended_at.isoformat(),
            },
        )
        json.dumps(payload)  # the whole payload must be JSON-serializable

        # Failed: includes exc_info from the supplied exception string
        failed_webhook = Webhook('http://example.com', 'failed')
        payload = failed_webhook.get_payload(job, exc_string='Traceback: division by zero')
        self.assertEqual(payload['status'], 'failed')
        self.assertEqual(payload['exc_info'], 'Traceback: division by zero')
        json.dumps(payload)

        # Failed: exc_info is None when no exception string is supplied
        payload = failed_webhook.get_payload(job)
        self.assertIsNone(payload['exc_info'])
        json.dumps(payload)

    def test_send_get(self):
        job = Job.create(say_hello, connection=self.connection)
        webhook = Webhook('http://example.com/hook', 'finished', headers={'X-Token': 'secret'}, timeout=3)

        with patch('rq.webhook.urlopen') as urlopen_mock:
            webhook.send(job)

        request = urlopen_mock.call_args.args[0]
        self.assertEqual(request.full_url, 'http://example.com/hook')
        self.assertEqual(request.get_method(), 'GET')
        self.assertIsNone(request.data)
        self.assertEqual(request.get_header('X-token'), 'secret')
        self.assertEqual(urlopen_mock.call_args.kwargs['timeout'], 3)

    def test_send_post(self):
        job = Job.create(say_hello, connection=self.connection)
        webhook = Webhook('http://example.com/hook', 'failed', method='POST')

        with patch('rq.webhook.urlopen') as urlopen_mock:
            webhook.send(job, exc_string='boom')

        request = urlopen_mock.call_args.args[0]
        self.assertEqual(request.get_method(), 'POST')
        self.assertEqual(request.get_header('Content-type'), 'application/json')
        body = json.loads(request.data.decode('utf-8'))
        self.assertEqual(body['job_id'], job.id)
        self.assertEqual(body['exc_info'], 'boom')

    def test_send_swallows_errors(self):
        """A dead endpoint must never raise out of send()"""
        job = Job.create(say_hello, connection=self.connection)
        webhook = Webhook('http://example.com/hook', 'finished')

        with patch('rq.webhook.urlopen', side_effect=ConnectionError('endpoint down')):
            webhook.send(job)  # should not raise


class JobWebhookTestCase(RQTestCase):
    def test_create_with_webhooks(self):
        webhooks = [
            Webhook('http://example.com/done', 'finished'),
            Webhook('http://example.com/fail', 'failed'),
        ]
        job = Job.create(say_hello, connection=self.connection, webhooks=webhooks)
        self.assertEqual(job.webhooks, webhooks)

    def test_create_rejects_invalid_webhooks(self):
        # A bare Webhook (not wrapped in a list)
        with self.assertRaises(TypeError):
            Job.create(say_hello, connection=self.connection, webhooks=Webhook('http://example.com', 'finished'))

        # A list containing non-Webhook items
        with self.assertRaises(TypeError):
            Job.create(say_hello, connection=self.connection, webhooks=['http://example.com'])

    def test_webhooks_survive_redis_round_trip(self):
        webhooks = [
            Webhook('http://example.com/done', 'finished', method='POST', headers={'X-A': 'b'}, timeout=5),
            Webhook('http://example.com/fail', 'failed'),
        ]
        job = Job.create(say_hello, connection=self.connection, webhooks=webhooks)
        job.save()

        fetched_job = Job.fetch(job.id, connection=self.connection)
        self.assertEqual(fetched_job.webhooks, webhooks)

    def test_missing_or_corrupted_webhooks_field_restores_empty_list(self):
        # Missing field (e.g. jobs saved by older RQ versions)
        job = Job.create(say_hello, connection=self.connection)
        job.save()
        self.assertEqual(Job.fetch(job.id, connection=self.connection).webhooks, [])

        # Corrupted field falls back to [] instead of raising
        self.connection.hset(job.key, 'webhooks', b'not-json')
        self.assertEqual(Job.fetch(job.id, connection=self.connection).webhooks, [])

    def test_send_webhooks(self):
        finished_webhook = Webhook('http://example.com/done', 'finished')
        failed_webhook = Webhook('http://example.com/fail', 'failed')
        job = Job.create(say_hello, connection=self.connection, webhooks=[finished_webhook, failed_webhook])

        # Only the matching webhook is sent
        with patch.object(Webhook, 'send', autospec=True) as send_mock:
            job.send_webhooks('finished')
        send_mock.assert_called_once_with(finished_webhook, job, exc_string=None)

        # JobStatus enum matches, and exc_string is forwarded to the matching webhook
        with patch.object(Webhook, 'send', autospec=True) as send_mock:
            job.send_webhooks(JobStatus.FAILED, exc_string='boom')
        send_mock.assert_called_once_with(failed_webhook, job, exc_string='boom')


class QueueWebhookTestCase(RQTestCase):
    """Webhooks must thread through every enqueue path onto the persisted job."""

    def setUp(self):
        super().setUp()
        self.queue = Queue(connection=self.connection)
        self.webhooks = [
            Webhook('http://example.com/done', 'finished'),
            Webhook('http://example.com/fail', 'failed'),
        ]

    def test_enqueue(self):
        job = self.queue.enqueue(say_hello, webhooks=self.webhooks)
        self.assertEqual(job.webhooks, self.webhooks)

    def test_enqueue_call(self):
        job = self.queue.enqueue_call(say_hello, webhooks=self.webhooks)
        self.assertEqual(job.webhooks, self.webhooks)

    def test_enqueue_at(self):
        job = self.queue.enqueue_at(datetime.now(timezone.utc), say_hello, webhooks=self.webhooks)
        self.assertEqual(job.webhooks, self.webhooks)

    def test_enqueue_in(self):
        job = self.queue.enqueue_in(timedelta(seconds=60), say_hello, webhooks=self.webhooks)
        self.assertEqual(job.webhooks, self.webhooks)

    def test_enqueue_many(self):
        job_data = Queue.prepare_data(say_hello, webhooks=self.webhooks)
        job = self.queue.enqueue_many([job_data])[0]
        self.assertEqual(job.webhooks, self.webhooks)

    def test_webhooks_persisted(self):
        """Webhooks survive enqueue + a fresh fetch from Redis."""
        job = self.queue.enqueue(say_hello, webhooks=self.webhooks)
        fetched_job = Job.fetch(job.id, connection=self.connection)
        self.assertEqual(fetched_job.webhooks, self.webhooks)


class WorkerWebhookTestCase(RQTestCase):
    """The worker dispatches matching webhooks once the job reaches a terminal state.

    These tests assert which webhooks the worker hands to `Webhook.send` (and with what
    `exc_string`); the HTTP request and error-swallowing mechanics are covered by `WebhookTestCase`.
    """

    def setUp(self):
        super().setUp()
        self.queue = Queue(connection=self.connection)
        self.worker = SimpleWorker([self.queue], connection=self.connection)
        self.finished_webhook = Webhook('http://example.com/done', 'finished')
        self.failed_webhook = Webhook('http://example.com/fail', 'failed')

    def test_terminal_status_fires_only_matching_webhook(self):
        # A successful job fires only the finished webhook
        self.queue.enqueue(say_hello, webhooks=[self.finished_webhook, self.failed_webhook])
        with patch.object(Webhook, 'send', autospec=True) as send_mock:
            self.worker.work(burst=True)
        send_mock.assert_called_once_with(self.finished_webhook, ANY, exc_string=ANY)

        # A failing job fires only the failed webhook, with the exception string forwarded
        self.queue.enqueue(div_by_zero, 1, webhooks=[self.finished_webhook, self.failed_webhook])
        with patch.object(Webhook, 'send', autospec=True) as send_mock:
            self.worker.work(burst=True)
        send_mock.assert_called_once_with(self.failed_webhook, ANY, exc_string=ANY)
        self.assertIn('ZeroDivisionError', send_mock.call_args.kwargs['exc_string'])

    def test_retry_exhausted_fires_failed_once(self):
        """Intermediate retry attempts don't fire; only the terminal failure does."""
        self.queue.enqueue(div_by_zero, 1, retry=Retry(max=1, interval=0), webhooks=[self.failed_webhook])

        # First attempt fails and is requeued for retry: the failed webhook must not fire
        with patch.object(Webhook, 'send', autospec=True) as send_mock:
            self.worker.work(burst=True, max_jobs=1)
        send_mock.assert_not_called()

        # Draining the requeued job reaches terminal failure: now it fires exactly once
        with patch.object(Webhook, 'send', autospec=True) as send_mock:
            self.worker.work(burst=True)
        send_mock.assert_called_once_with(self.failed_webhook, ANY, exc_string=ANY)

    def test_retry_then_success_skips_failed(self):
        """A job that fails then succeeds fires finished, never failed."""
        self.queue.enqueue(
            fail_while_retries_remain,
            retry=Retry(max=1, interval=0),
            webhooks=[self.finished_webhook, self.failed_webhook],
        )
        with patch.object(Webhook, 'send', autospec=True) as send_mock:
            self.worker.work(burst=True)
        send_mock.assert_called_once_with(self.finished_webhook, ANY, exc_string=ANY)

    def test_return_based_retry_exhaustion_fires_failed(self):
        """A job returning Retry until exhausted fires the failed webhook on terminal failure."""
        self.queue.enqueue(returns_retry, webhooks=[self.finished_webhook, self.failed_webhook])
        with patch.object(Webhook, 'send', autospec=True) as send_mock:
            self.worker.work(burst=True)
        send_mock.assert_called_once_with(self.failed_webhook, ANY, exc_string=ANY)


class SyncWebhookTestCase(RQTestCase):
    """Jobs executed synchronously (is_async=False) also fire webhooks, since they bypass the worker."""

    def setUp(self):
        super().setUp()
        self.queue = Queue(connection=self.connection, is_async=False)
        self.finished_webhook = Webhook('http://example.com/done', 'finished')
        self.failed_webhook = Webhook('http://example.com/fail', 'failed')

    def test_finished_fires_only_finished(self):
        with patch.object(Webhook, 'send', autospec=True) as send_mock:
            self.queue.enqueue(say_hello, webhooks=[self.finished_webhook, self.failed_webhook])
        send_mock.assert_called_once_with(self.finished_webhook, ANY, exc_string=ANY)

    def test_failed_fires_only_failed_with_exc_info(self):
        with patch.object(Webhook, 'send', autospec=True) as send_mock:
            self.queue.enqueue(div_by_zero, 1, webhooks=[self.finished_webhook, self.failed_webhook])
        send_mock.assert_called_once_with(self.failed_webhook, ANY, exc_string=ANY)
        self.assertIn('ZeroDivisionError', send_mock.call_args.kwargs['exc_string'])
