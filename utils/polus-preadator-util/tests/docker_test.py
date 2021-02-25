import unittest, multiprocessing
from pathlib import Path
from preadator import ProcessManager

class DockerTest(unittest.TestCase):
    
    def test_1_thread(self):
        
        self.assertEqual(1,ProcessManager.num_threads())
        
    @unittest.expectedFailure
    def test_all_thread(self):
        
        self.assertEqual(multiprocessing.cpu_count(),ProcessManager.num_threads())   
        
if __name__=="__main__":
    
    unittest.main()