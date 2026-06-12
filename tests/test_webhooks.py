import json
from datetime import datetime, timezone
from unittest.mock import patch

from rq.job import Job, JobStatus
from rq.queue import Queue
from rq.results import Result
from rq.webhook import Webhook
from tests import RQTestCase, min_redis_version

from .fixtures import say_hello


class WebhookTestCase(RQTestCase):
    def test_init_defaults(self):
        webhook = Webhook('http://example.com/hook', 'finished')
        self.assertEqual(webhook.url, 'http://example.com/hook')
        self.assertEqual(webhook.job_status, 'finished')
        self.assertEqual(webhook.method, 'GET')
        self.assertIsNone(webhook.headers)
        self.assertEqual(webhook.timeout, 10)

    def test_init_full(self):
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
        with self.assertRaises(ValueError):
            Webhook('http://example.com', JobStatus.STOPPED)
        with self.assertRaises(ValueError):
            Webhook('http://example.com', ['finished'])

    def test_method_validation(self):
        with self.assertRaises(ValueError):
            Webhook('http://example.com', 'finished', method='PUT')
        with self.assertRaises(ValueError):
            Webhook('http://example.com', 'finished', method='DELETE')
        with self.assertRaises(ValueError):
            Webhook('http://example.com', 'finished', method='get')
        with self.assertRaises(ValueError):
            Webhook('http://example.com', 'finished', method='post')
        with self.assertRaises(ValueError):
            Webhook('http://example.com', 'finished', method=None)

    def test_headers_validation(self):
        with self.assertRaises(TypeError):
            Webhook('http://example.com', 'finished', headers=[('X-Token', 'secret')])

    def test_timeout_validation(self):
        with self.assertRaises(ValueError):
            Webhook('http://example.com', 'finished', timeout=0)
        with self.assertRaises(ValueError):
            Webhook('http://example.com', 'finished', timeout=-1)
        with self.assertRaises(ValueError):
            Webhook('http://example.com', 'finished', timeout='10')
        with self.assertRaises(ValueError):
            Webhook('http://example.com', 'finished', timeout=1.5)

    def test_to_dict(self):
        webhook = Webhook('http://example.com', JobStatus.FAILED, method='POST', timeout=5)
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

    def test_from_dict_round_trip(self):
        webhook = Webhook('http://example.com', 'finished', method='POST', headers={'X-A': 'b'}, timeout=3)
        self.assertEqual(Webhook.from_dict(webhook.to_dict()), webhook)

    def test_from_dict_defaults(self):
        webhook = Webhook.from_dict({'url': 'http://example.com', 'job_status': 'failed'})
        self.assertEqual(webhook, Webhook('http://example.com', 'failed'))

    def test_from_dict_revalidates(self):
        with self.assertRaises(ValueError):
            Webhook.from_dict({'url': 'http://example.com', 'job_status': 'stopped'})

    def test_build_payload_finished(self):
        job = Job.create(say_hello, connection=self.connection)
        job.enqueued_at = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        job.ended_at = datetime(2026, 1, 1, 12, 0, 5, tzinfo=timezone.utc)
        job._result = {'answer': 42}

        payload = Webhook('http://example.com', 'finished').build_payload(job)
        self.assertEqual(
            payload,
            {
                'job_id': job.id,
                'func_name': 'tests.fixtures.say_hello',
                'status': 'finished',
                'enqueued_at': job.enqueued_at.isoformat(),
                'ended_at': job.ended_at.isoformat(),
                'result': {'answer': 42},
            },
        )
        json.dumps(payload)  # the whole payload must be JSON-serializable

    def test_build_payload_unserializable_result(self):
        """Results that can't be JSON-serialized fall back to str()"""
        job = Job.create(say_hello, connection=self.connection)
        job._result = object()

        payload = Webhook('http://example.com', 'finished').build_payload(job)
        self.assertEqual(payload['result'], str(job._result))
        json.dumps(payload)

    @min_redis_version((5, 0, 0))
    def test_build_payload_failed(self):
        queue = Queue(connection=self.connection)
        job = queue.enqueue(say_hello)
        Result.create_failure(job, ttl=10, exc_string='Traceback: division by zero')

        payload = Webhook('http://example.com', 'failed').build_payload(job)
        self.assertEqual(payload['status'], 'failed')
        self.assertEqual(payload['exc_info'], 'Traceback: division by zero')
        self.assertNotIn('result', payload)
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
        job._result = 'ok'
        webhook = Webhook('http://example.com/hook', 'finished', method='POST')

        with patch('rq.webhook.urlopen') as urlopen_mock:
            webhook.send(job)

        request = urlopen_mock.call_args.args[0]
        self.assertEqual(request.get_method(), 'POST')
        self.assertEqual(request.get_header('Content-type'), 'application/json')
        body = json.loads(request.data.decode('utf-8'))
        self.assertEqual(body['job_id'], job.id)
        self.assertEqual(body['result'], 'ok')

    def test_send_swallows_errors(self):
        """A dead endpoint must never raise out of send()"""
        job = Job.create(say_hello, connection=self.connection)
        webhook = Webhook('http://example.com/hook', 'finished')

        with patch('rq.webhook.urlopen', side_effect=ConnectionError('endpoint down')):
            webhook.send(job)  # should not raise
