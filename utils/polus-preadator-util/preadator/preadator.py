import multiprocessing, queue, typing, logging, atexit, os, signal
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, wait, FIRST_EXCEPTION, TimeoutError
from concurrent.futures.process import BrokenProcessPool

logging.basicConfig(format='%(asctime)s - %(name)-8s - %(levelname)-8s - %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S')

try:
    # On Linux, we can detect how many cores are assigned to this process.
    # This is especially useful when running in a Docker container, when the
    # number of cores is intentionally limited.
    num_threads = len(os.sched_getaffinity(0))
except:
    # Default back to multiprocessing cpu_count, which is always going to count
    # the total number of cpus
    num_threads = multiprocessing.cpu_count()
    
num_processes = max([num_threads//2,1])

process_update_period = 30
thread_update_period = 10

_manager_args = {
    'num_processes': num_processes,
    'num_threads': num_threads,
    'threads_per_process': num_threads//num_processes,
    'threads_per_request': num_threads//num_processes,
    'running': False,
    'process_queue': None,
    'process_ids': None,
    'process_names': None,
    'log_level': logging.WARNING
}

@atexit.register
def destroy_executors():
    
    errors = []
    if ProcessManager._thread_executor != None:
        ProcessManager.logger.info('Shutting down thread executor...')
        ProcessManager._thread_executor.shutdown(True)
        
        if ProcessManager._threads != None:
            for thread in ProcessManager._threads:
                try:
                    e = thread.exception(timeout=0)
                    if e != None:
                        errors.append(e)
                except TimeoutError:
                    pass
    
    if ProcessManager._processes != None:
        
        ProcessManager.logger.info('Checking for errors...')
        for process in ProcessManager._processes:
            try:
                e = process.exception(timeout=0)
                if e != None:
                    errors.append(e)
            except TimeoutError:
                pass
            
        ProcessManager.logger.info('Shutting down process executor...')
        ProcessManager._process_executor.shutdown(False)
    
        ProcessManager.logger.info('Killing child processes...')
        try:
            while True:
                try:
                    os.kill(_manager_args['process_ids'].get(timeout=0),signal.SIGKILL)
                except ProcessLookupError:
                    pass
        except queue.Empty:
            pass

    for e in errors:
        ProcessManager.logger.error(e)
        raise e
    
class QueueLock:
    
    _name = 'Queue'
    
    def __init__(self,
                 queue: typing.Union[multiprocessing.Queue,queue.Queue]):
        
        self.queue = queue
        self.thread_count = 0
        
    def __enter__(self) -> int:
        """Handle entrace to a process/thread task

        Returns:
            Returns number of threads available for work
        """
        self.lock()
        return self
    
    def lock(self):
        self.logger.debug(f'Getting {self._name} from queue...')
        self.count = self.queue.get()
        self.logger.info(f'Acquired {self.count} {self._name} from queue!')
    
    def release(self):
        raise NotImplementedError('QueueLock should be inherited by a class, and _put() should be overriden.')
    
    def __del__(self):
        """Handle thread deletion
        """
        if self.count != 0:
            self.logger.debug(f'Deleting {self.__class__.__name__} object, cleaning up {self._name}...')
            self.release()
    
    def __exit__(self, type_class, value, traceback):
        """Handle exit from the context manager
        This code runs when exiting a `with` statement.
        """
        self.release()
        self.count = 0
        
class ThreadLock(QueueLock):
    
    _name = 'threads'
    logger = logging.getLogger('pread.ThreadLock')
    
    def release(self):
        self.logger.info(f'Releasing {self.count} threads...')
        self.queue.put(self.count)
        
class ProcessLock(QueueLock):
    
    _name = 'processes'
    logger = logging.getLogger('pread.ProcessLock')
    
    def release(self):
        
        # We may have stolen threads from other processes, so put them back
        try:
            threads_returned = 0
            while True:
                threads_returned += ProcessManager._thread_queue.get(timeout=0)
                
        except queue.Empty:
            for _ in range(0,threads_returned,ProcessManager.threads_per_process()):
                self.logger.info(f'Freeing process...')
                self.queue.put(ProcessManager.threads_per_process(),timeout=0)
                self.logger.info(f'Process freed!')
            ProcessManager._active_threads = ProcessManager.threads_per_process()
            ProcessManager._thread_queue.put(ProcessManager.threads_per_request())
            self.logger.info(f'Returned {threads_returned} threads!!')

class ProcessManager():
    
    _name = '__process_manager_queue__'
    _process_executor = None
    _thread_executor = None
    _threads = None
    _processes = None
    _active_threads = 0
    
    _main_process_name = "pread.Manager"
    
    # Logger for debugging the ProcessManager
    logger = logging.getLogger("pread.Manager")
    
    # Logger for displaying messages in the main process
    _process_name = "main"
    main_logger = logging.getLogger(_process_name)
    main_logger.setLevel(logging.INFO)
    
    # Logger for displaying messages within child processes, this will be the
    # same as main_logger is init_processes is not called.
    process_logger = main_logger
    
    # Modifier added to process messages to track individual jobs
    _job_name = ''
    
    @classmethod
    def _get_property(cls,name,value):
        if value != None and not ProcessManager.running():
            _manager_args[name] = value
        return _manager_args[name]
    
    @classmethod
    def job_name(cls,value = None):
        if value != None:
            cls._job_name = value + ": "
        return cls._job_name
    
    @classmethod
    def num_processes(cls,value = None):
        return ProcessManager._get_property('num_processes',value)
        
    @classmethod
    def num_threads(cls,value = None):
        return ProcessManager._get_property('num_threads',value)
        
    @classmethod
    def threads_per_process(cls,value = None):
        return ProcessManager._get_property('threads_per_process',value)
    
    @classmethod
    def threads_per_request(cls,value = None):
        return ProcessManager._get_property('threads_per_request',value)
        
    @classmethod
    def running(cls,value = None):
        assert value == None, 'Cannot set running property.'
        return ProcessManager._get_property('running',value)
    
    @classmethod
    def process_queue(cls, value = None):
        assert value == None, 'Cannot set process queue.'
        return ProcessManager._get_property('process_queue',value)
    
    _thread_queue = None
    
    def __init__(self,*args,**kwargs):
        
        raise PermissionError('The ProcessManager is a static class and cannot be instantiated.')
    
    @staticmethod
    def _initializer(kwargs):
        
        # Set the global variables
        for k,v in kwargs.items():
                
            globals()[k] = v
            
            ProcessManager._running = True
            
        # Initialize the logger with the appropriate name
        ProcessManager.logger.setLevel(_manager_args['log_level'])

        # Initialize the logger with the appropriate name
        _process_name = _manager_args['process_names'].get(timeout=0)
        ProcessManager.process_logger = logging.getLogger(_process_name)
        ProcessManager.process_logger.setLevel(logging.INFO)

        # Put the process ids in the Queue to share with the main process
        _manager_args['process_ids'].put(os.getpid())
        ProcessManager.logger.debug(os.getpid())

        ProcessManager.init_threads()
        
    @classmethod
    def log(cls,msg):
        ProcessManager.process_logger.info(ProcessManager._job_name + msg)
    
    @classmethod
    def init_processes(cls,
                       name=None,
                       pnames=None,
                       **kwargs):
        
        # Change the main process name if requested
        if name != None:
            if not isinstance(name,str):
                ProcessManager.logger.error("The main process name must be a string.",
                                            TypeError)
            ProcessManager.main_logger = logging.getLogger(name)
            ProcessManager.main_logger.setLevel(logging.INFO)
        
        # Validate the process names, generate if they don't exist
        if isinstance(pnames,list):
            if len(pnames) != ProcessManager.num_processes():
                ProcessManager.logger.error(f'There must be as many names as processes ({ProcessManager.num_processes()})',
                                            TypeError)
        elif isinstance(pnames,str) or pnames == None:
            if pnames == None:
                pnames = 'proc'
            pnames = [pnames + ' ' + str(i+1) for i in range(ProcessManager.num_processes())]
        else:
            raise TypeError('names must be a string, list, or None.')
            
        # Create the process name queue
        _manager_args['process_names'] = multiprocessing.Queue(ProcessManager.num_processes())
        for name in pnames:
            _manager_args['process_names'].put(name)
        
        # Make sure the ProcessManager isn't already running a ProcessPool
        assert not ProcessManager.running(), \
            "The process manager has already been initialized. Try shutting down and restarting."
        
        # Start the ProcessPoolExecutor
        _manager_args['running'] = True
        ProcessManager.logger.debug(f'Starting process_queue with {ProcessManager.num_processes()} processes...')
        
        # Create the process queue and populate it
        _manager_args['process_queue'] = multiprocessing.Queue(ProcessManager.num_processes())
        for p in range(ProcessManager.num_processes()):
            _manager_args['process_queue'].put(ProcessManager.threads_per_process())
            
        # Create the process id queue
        _manager_args['process_ids'] = multiprocessing.Queue()
        
        kwargs['_manager_args'] = _manager_args
        
        ProcessManager._process_executor = ProcessPoolExecutor(ProcessManager.num_processes(),
                                                    initializer = ProcessManager._initializer,
                                                    initargs=(kwargs,))
        ProcessManager._processes = []
        
    @classmethod
    def init_threads(cls):
        
        ProcessManager._thread_executor = ThreadPoolExecutor(ProcessManager.num_threads())
        ProcessManager._threads = []
        
        if ProcessManager.running():
            threads = ProcessManager.threads_per_process()
        else:
            threads = ProcessManager.num_threads()//ProcessManager.threads_per_request()
            
        ProcessManager.logger.debug(f'Starting the thread queue with size {threads}')
        ProcessManager._thread_queue = queue.Queue(ProcessManager.num_threads())
        for _ in range(0,threads,ProcessManager.threads_per_request()):
            ProcessManager._thread_queue.put(ProcessManager.threads_per_request())
            
        ProcessManager._active_threads = threads
    
    @classmethod
    def process(cls,name = None, display = True):
        
        if name != None:
            ProcessManager._job_name = name + ': '
        
        return ProcessLock(ProcessManager.process_queue())
    
    @classmethod
    def thread(cls):
        return ThreadLock(ProcessManager._thread_queue)
        
    @classmethod
    def submit_process(cls,process,*args,**kwargs):
        
        ProcessManager._processes.append(ProcessManager._process_executor.submit(process,*args,**kwargs))
        
    @classmethod
    def submit_thread(cls,thread,*args,**kwargs):
        
        ProcessManager._threads.append(ProcessManager._thread_executor.submit(thread,*args,**kwargs))
        
    @classmethod
    def join_processes(cls,update_period=process_update_period):
                
        # Wait for the processes to finish
        not_done = [None]
        while len(not_done) > 0:
            
            # TODO: _threads should be switched to not_done reduce overhead
            # NOTE: This will require changing the progress updates
            done, not_done = wait(ProcessManager._processes,timeout=update_period,
                                  return_when=FIRST_EXCEPTION)
            
            for p in done:
                e = p.exception()
                if e != None:
                    atexit.unregister(destroy_executors)
                    destroy_executors()
            
            ProcessManager.main_logger.info(f'Processes {100*len(done)/len(ProcessManager._processes):6.2f}% complete')
            
    @classmethod
    def join_threads(cls,update_period=thread_update_period):
                
        # Wait for the processes to finish
        not_done = [None]
        while len(not_done) > 0:
            
            # TODO: _threads should be switched to not_done reduce overhead
            # NOTE: This will require changing the progress updates
            done, not_done = wait(ProcessManager._threads,timeout=update_period,
                                  return_when=FIRST_EXCEPTION)
            for p in done:
                e = p.exception()
                if e != None:
                    atexit.unregister(destroy_executors)
                    destroy_executors()
            
            ProcessManager.process_logger.info(f'{ProcessManager._job_name}{100*len(done)/len(ProcessManager._threads):6.2f}% complete')
            
            # Steal threads from available processes
            if ProcessManager._active_threads < _manager_args['num_threads'] \
                and ProcessManager.process_queue() != None:
                    
                try:
                    new_threads = 0
                    while ProcessManager.process_queue().get(timeout=0):
                        for _ in range(0,ProcessManager.threads_per_process(),ProcessManager.threads_per_request()):
                            ProcessManager._thread_queue.put(ProcessManager.threads_per_request(),timeout=0)
                            new_threads += ProcessManager.threads_per_request()
                            
                except queue.Empty:
                    if new_threads > 0:
                        ProcessManager.logger.info(f'{ProcessManager._process_name}: Added {new_threads} threads to the queue.')
                        ProcessManager._active_threads += new_threads
            