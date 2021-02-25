import unittest
from pathlib import Path
from preadator import ProcessManager

class ThreadTest(unittest.TestCase):
    
    @unittest.expectedFailure
    def test_thread_error(self):
        
        ProcessManager.init_threads()
        
        def thread_error():
            raise ChildProcessError("Expected ChildProcessError during thread test.")
        
        ProcessManager.submit_thread(thread_error)
        
        ProcessManager.join_threads()
        
    def test_thread_success(self):
        
        ProcessManager.init_threads()
        
        def thread_successful():
            return True
        
        ProcessManager.submit_thread(thread_successful)
        
        ProcessManager.join_threads()      
        
if __name__=="__main__":
    
    unittest.main()