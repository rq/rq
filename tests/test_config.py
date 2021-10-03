from rq.config import Config, DEFAULT_CONFIG
from rq.serializers import DefaultSerializer, JSONSerializer
from rq.worker import Worker, SimpleWorker, RoundRobinWorker

from tests import RQTestCase


class ConfigTestCase(RQTestCase):
    def test_config(self):
        self.assertEqual(DEFAULT_CONFIG.worker_class, Worker)
        self.assertEqual(DEFAULT_CONFIG.serializer, DefaultSerializer)

        config_0 = Config(worker_class='rq.SimpleWorker', serializer='rq.serializers.JSONSerializer')
        self.assertEqual(config_0.worker_class, SimpleWorker)
        self.assertEqual(config_0.serializer, JSONSerializer)
        self.assertEqual(config_0.job_class, DEFAULT_CONFIG.job_class)

        config_1 = Config(worker_class='rq.worker.RoundRobinWorker', template=config_0)
        self.assertEqual(config_1.worker_class, RoundRobinWorker)
        self.assertEqual(config_1.serializer, JSONSerializer)
        self.assertEqual(config_1.job_class, DEFAULT_CONFIG.job_class)
