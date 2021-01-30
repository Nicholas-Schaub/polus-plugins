import multiprocessing, queue, typing, logging
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, wait

num_threads = multiprocessing.cpu_count()
num_processes = max([num_threads//2,1])

process_update_period = 30
thread_update_period = 10

logging.basicConfig(format='%(asctime)s - %(name)-8s - %(levelname)-8s - %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S')

class QueueLock:
    
    def __init__(self,
                 queue: typing.Union[multiprocessing.Queue,queue.Queue]):
        
        self.queue = queue
        self.thread_count = []
        self.thread_max = ProcessManager.thread_max
        
    def get(self):
        if sum(thread_count) < self.thread_max:
            return 0
        
        try:
            while True:
                self.thread_count.append(self.queue.get(timeout=0))
        except queue.Empty:
            return sum(self.thread_count)
        
    def __enter__(self) -> int:
        """Handle entrace to a process/thread task

        Returns:
            Returns number of threads available for work
        """
        self.thread_count.append(self.queue.get())
        return self
    
    def __del__(self):
        """Handle thread deletion
        """
        for thread in self.thread_count:
            self.queue.put(thread)
        
    
    def __exit__(self, type_class, value, traceback):
        """Handle exit from the context manager
        This code runs when exiting a `with` statement.
        """
        for thread in self.thread_count:
            self.queue.put(thread)

class ProcessManager:
    
    _name = '__process_manager_queue__'
    _process_executor = None
    _thread_executor = None
    _threads = None
    _processes = None
    
    _manager_args = {
        'num_processes': num_processes,
        'num_threads': num_threads,
        'threads_per_process': num_threads//num_processes,
        'threads_per_request': num_threads//num_processes,
        'running': False,
        'process_queue': None,
        'log_level': logging.INFO
    }
    
    _active_threads = 0
    
    logger = logging.getLogger("pread")
    
    @property
    def num_processes(self):
        self._manager_args['num_processes']
        
    @property
    def num_threads(self):
        self._manager_args['num_threads']
        
    @property
    def threads_per_process(self):
        self._manager_args['threads_per_process']
    
    @property
    def threads_per_request(self):
        self._manager_args['threads_per_request']
        
    @property
    def running(self):
        self._manager_args['running']
    
    _thread_queue = None
    
    def __init__(self,*args,**kwargs):
        
        raise PermissionError('The ProcessManager is a static class and cannot be instantiated.')
    
    @staticmethod
    def _initializer(kwargs):
        
        for k,v in kwargs.items():
            if k.startswith(ProcessManager._name):
                ProcessManager._manager_args[k] = v
                continue
            
            ProcessManager._thread_queue = ThreadPoolExecutor()
                
            globals()[k] = v
            
            ProcessManager._running = True
    
    @classmethod
    def init_processes(cls,**kwargs):
        
        assert not ProcessManager.running, "The process manager has already been initialized. Try shutting down and restarting."
        
        ProcessManager._manager_args['running'] = True
        
        self._process_queue = multiprocessing.Queue(cls._num_processes)
        for p in range(cls._num_processes):
            cls._queue.put(cls._threads_per_process)
        
        for k,v in ProcessManager._manager_args.items():
            kwargs[self._name + k] = v
        
        cls._process_executor = ProcessPoolExecutor(cls._num_processes,
                                                    initializer = cls._initializer,
                                                    initargs=(kwargs,))
        
    @classmethod
    def init_threads(cls):
        
        self._thread_executor = ThreadPoolExecutor(ProcessManager.num_threads)
        self._threads = []
        
        if ProcessManager.running:
            threads = ProcessManager.threads_per_process
        else:
            threads = ProcessManager.num_threads
        for _ in range(threads):
            ProcessManager._thread_queue.put(ProcessManager.threads_per_request)
    
    @classmethod
    def process(cls):
        return QueueLock(cls._process_queue)
    
    @classmethod
    def thread(cls):
        return QueueLock(cls._thread_queue)
        
    @classmethod
    def submit_process(cls,process,*args,**kwargs):
        
        cls._processes.append(cls._process_executor.submit(process,*args,**kwargs))
        
    @classmethod
    def submit_thread(cls,thread,*args,**kwargs):
        
        cls._threads.append(cls._thread_executor.submit(thread,*args,**kwargs))
        
    @classmethod
    def join_process(cls,update_period=process_update_period):
        
        # Steal threads from available processes
        if ProcessManager._active_processes < ProcessManager._manager_args['num_threads']:
            try:
                new_threads = 0
                while True:
                    new_threads += cls._process_queue.get()
            except queue.Empty:
                cls._active_threads += new_threads
                
        # Wait for the processes to finish
        not_done = []
        while len(not_done) > 0:
            
            done, not_done = wait(cls._processes,timeout=process_update_period)
            
            cls.logger.info(f'{len(done)/len(cls._processes):6.2f}% completed')
            
    @classmethod
    def join_thread(cls,update_period=process_update_period):
        
        # Steal threads from available processes
        if ProcessManager._active_processes < ProcessManager._manager_args['num_threads']:
            try:
                new_threads = 0
                while True:
                    new_threads += cls._thread_queue.get()
            except queue.Empty:
                cls._active_threads += new_threads
                
        # Wait for the processes to finish
        not_done = []
        while len(not_done) > 0:
            
            done, not_done = wait(cls._processes,timeout=process_update_period)
            
            cls.logger.info(f'{len(done)/len(cls._processes):6.2f}% completed')
        