# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)

from tests import RQTestCase
from rq.utils import transfer_timeout


class TestUtils(RQTestCase):
    def test_transfer_timeout(self):
        """Ensure function transfer_timeout works correctly"""
        self.assertEqual(12, transfer_timeout(12))
        self.assertEqual(12, transfer_timeout('12'))
        self.assertEqual(12, transfer_timeout('12s'))
        self.assertEqual(720, transfer_timeout('12m'))
        self.assertEqual(3600, transfer_timeout('1h'))
