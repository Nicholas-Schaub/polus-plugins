
import filepattern
import argparse, logging
import numpy as np
import typing, os
from pathlib import Path
import vaex

# Import environment variables
POLUS_LOG = getattr(logging,os.environ.get('POLUS_LOG','INFO'))

# Initialize the logger
logging.basicConfig(format='%(asctime)s - %(name)-8s - %(levelname)-8s - %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S')
logger = logging.getLogger("main")
logger.setLevel(POLUS_LOG)

def csv2parquet(input_path: Path,
                output_path: Path
                ) -> np.ndarray:
    """Convert a csv file to Apache feather file
    """
    
    # Get the number of columns in the csv
    with open(input_path,'r') as fr:
        ncols = len(fr.readline().split(','))
    
    # Set the chunk size to get roughly 16MB at a time, or at least 1 row
    chunk_size = max([2**24 // ncols,1])
    
    # Load the file and convert to hdf5 to make reading scalable
    logger.info('reading file...')
    csv = vaex.from_csv(input_path,convert=True,chunk_size=chunk_size)
    
    # Write to parquet file
    logger.info('writing file...')
    csv.export_feather(output_path)
    csv.close()
    
    # Remove intermediate files
    logger.info('removing intermediates...')
    os.unlink(input_path.with_suffix('.csv.yaml'))
    os.unlink(input_path.with_suffix('.csv.hdf5'))
    logger.info('done.')
    
def main(inpDir: Path,
         filePattern: str,
         outDir: Path,
         ) -> None:
    """ Main execution function
    
    All functions in your code must have docstrings attached to them, and the
    docstrings must follow the Google Python Style:
    https://www.sphinx-doc.org/en/master/usage/extensions/example_google.html
    """
    
    pattern = filePattern if filePattern is not None else '.*.csv'
    fp = filepattern.FilePattern(inpDir,pattern)
    
    for files in fp:
        
        # get the first file
        input_file = files.pop()['file']
        output_file = outDir.joinpath(input_file.name.replace('.csv','.feather'))
        
        logger.info(f'Converting file: {input_file.name}')
        
        csv2parquet(input_file,output_file)

if __name__=="__main__":

    ''' Argument parsing '''
    logger.info("Parsing arguments...")
    parser = argparse.ArgumentParser(prog='main', description='Convert data from csv to feather.')
    
    # Input arguments
    parser.add_argument('--inpDir', dest='inpDir', type=str,
                        help='Input csv collection to be processed by this plugin', required=True)
    parser.add_argument('--filePattern', dest='filePattern', type=str, default='.*.csv',
                        help='Filename pattern used to separate data', required=False)
    # Output arguments
    parser.add_argument('--outDir', dest='outDir', type=str,
                        help='Output collection', required=True)
    
    # Parse the arguments
    args = parser.parse_args()
    inpDir = Path(args.inpDir)
    logger.info('inpDir = {}'.format(inpDir))
    filePattern = args.filePattern
    logger.info('filePattern = {}'.format(filePattern))
    outDir = Path(args.outDir)
    logger.info('outDir = {}'.format(outDir))
    
    main(inpDir=inpDir,
         filePattern=filePattern,
         outDir=outDir)