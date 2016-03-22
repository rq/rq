from rq.utils import build_key
from tests import RQTestCase


class TestHelpers(RQTestCase):

    def test_build_key(self):
        """Ensure the redis key is correctly built when there's a namespace present"""
        self.assertEqual(build_key("rq:job", None), "rq:job")
        self.assertEqual(build_key("rq:job", "myprefix"), "myprefix:rq:job")
        self.assertEqual(build_key("rq:jobs:", "myprefix"), "myprefix:rq:jobs:")
