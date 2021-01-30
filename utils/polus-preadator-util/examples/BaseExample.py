import time
from random import random

delay = 1

def CountingThread(value,delay):
    
    print(value)
    
    time.sleep(delay)

def CountingProcess(name,phrase=[]):

    print(f'{name} starts speaking...')
    
    for v in phrase:
        
        CountingThread(v,delay*random())
    
    print(f'{name} is done speaking.')
    print()
        
if __name__=='__main__':
    
    values = [('Aragorn','If by my life or death I can protect you, I will. You have my sword.'.split(' ')),
              ('Legolas', 'And you have my bow.'.split(' ')),
              ('Gimli', 'And my axe.'.split(' ')),
              ('Pippin', 'What about second breakfast? Elevenses?'.split(' ')),
              ('Gandalf', 'You shall not pass!'.split(' '))]
    
    start = time.time()
    for value in values:
        
        CountingProcess(*value)
    print(f'Finished processing all in {time.time() - start:.2f}s!')