from unittest import TestCase
from rq.scripts import read_config_file

class TestScripts(TestCase):
    def test_config_file(self):
        settings = read_config_file("tests.dummy_settings")
        self.assertIn("REDIS_HOST", settings)
        self.assertEqual(settings['REDIS_HOST'], "testhost.example.com")
