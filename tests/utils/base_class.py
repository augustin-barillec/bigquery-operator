import unittest
from tests.utils import exist, empty, create, delete, pandas_equal


class BaseClassTest(unittest.TestCase):

    @staticmethod
    def assert_pandas_equal(expected, computed):
        pandas_equal.assert_equal(expected, computed)

    def setUp(self):
        if exist.dataset_exists():
            empty.empty_dataset()
            delete.delete_dataset()
        create.create_dataset()

    def tearDown(self):
        empty.empty_dataset()
        delete.delete_dataset()
