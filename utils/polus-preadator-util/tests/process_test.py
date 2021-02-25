import unittest
from pathlib import Path
from preadator import ProcessManager
        
def child_process_error():
    raise ChildProcessError("Expected ChildProcessError during Process test.")

def child_thread():
    raise ChildProcessError("Expected ChildProcessError in thread during Process test.")

def child_process():
    ProcessManager.submit_thread(child_thread)
    ProcessManager.join_threads()

class ProcessTest(unittest.TestCase):
    
    @unittest.expectedFailure
    def test_process_error(self):
        
        ProcessManager.init_processes()
        
        ProcessManager.submit_process(child_process_error)
        
        ProcessManager.join_processes()
        
    @unittest.expectedFailure
    def test_process_thread_error(self):
        
        ProcessManager.init_processes()
        
        ProcessManager.submit_process(child_process)
        
        ProcessManager.join_processes()