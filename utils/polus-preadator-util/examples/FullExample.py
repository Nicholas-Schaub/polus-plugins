import time
from random import random
from preadator import ProcessManager

delay = 5

def CountingThread(value,delay):
    
    with ProcessManager.thread():
    
        # print(value)
        ProcessManager.log(value)
        
        time.sleep(delay)

def CountingProcess(name,phrase=[]):
    
    with ProcessManager.process(name):
        
        # print(f'{name} starts speaking...')
        ProcessManager.log(f'starts speaking...')
        
        for v in phrase:
            
            ProcessManager.submit_thread(CountingThread,v,delay*random())
                    
        ProcessManager.join_threads()
        
        # print(f'{name} is done speaking.')
        ProcessManager.log(f'done speaking.')
        
if __name__=='__main__':
    
    ProcessManager.init_processes()
    
    values = [('Aragorn','If by my life or death I can protect you, I will. You have my sword.'.split(' ')),
              ('Legolas', 'And you have my bow.'.split(' ')),
              ('Gimli', 'And my axe.'.split(' ')),
              ('Pippin', 'What about second breakfast? Elevenses?'.split(' ')),
              ('Gandalf', 'You shall not pass!'.split(' '))]
    
    start = time.time()
    for value in values:
        
        ProcessManager.submit_process(CountingProcess,*value)
    
    ProcessManager.join_processes()
    print(f'Finished processing all in {time.time() - start:.2f}s!')