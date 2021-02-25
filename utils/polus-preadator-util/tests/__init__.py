from unittest import TestSuite
from .thread_test import ThreadTest
from .process_test import ProcessTest

test_cases = (ProcessTest,ThreadTest)

def load_tests(loader, tests, pattern):
    suite = TestSuite()
    for test_class in test_cases:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    return suite