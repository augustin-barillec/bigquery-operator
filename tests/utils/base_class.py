import unittest
from tests.utils import dataframe, dataset


class BaseClassTest(unittest.TestCase):

    @staticmethod
    def assert_dataframe_equal(expected, computed):
        dataframe.assert_equal(expected, computed)

    def setUp(self):
        dataset.delete_dataset()
        dataset.create_dataset()

    def tearDown(self):
        dataset.delete_dataset()
