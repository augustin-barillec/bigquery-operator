import unittest
from tests.test_table_methods import TableMethodsWithoutApiCallsTest

suite = unittest.TestSuite()
suite.addTest(TableMethodsWithoutApiCallsTest('test_build_table_id'))
unittest.TextTestRunner(verbosity=2).run(suite)
