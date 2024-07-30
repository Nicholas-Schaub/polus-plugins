import argparse, logging, math, filepattern, time, queue
from bfio import BioReader, BioWriter
import pathlib
from preadator import ProcessManager

logging.basicConfig(format='%(asctime)s - %(name)-8s - %(levelname)-8s - %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S')

# length/width of the chunk each _merge_layers thread processes at once
chunk_size = 8192

# Units for conversion
UNITS = {'m':  10**9,
         'cm': 10**7,
         'mm': 10**6,
         'µm': 10**3,
         'nm': 1,
         'Å':  10**-1}

def _transfer_thread(br_path: pathlib.Path, bw_path: pathlib.Path, metadata, ys, ye, xs, xe, z, zi):

    with ProcessManager.process():
        
        with BioReader(br_path, backend="zarr") as br:
        
            with BioWriter(bw_path, append=True, metadata=metadata, backend="zarr") as bw:
            
                bw[ys:ye,xs:xe,zi:zi+1,...] = br[ys:ye,xs:xe,z:z+1,...]

def _merge_layers(input_files,output_path,file_offset,num_files):
    
    # with ProcessManager.process(output_path.name):

    # Get the number of layers to stack
    z_size = 0
    for f in input_files:
        with BioReader(f['file']) as br:
            z_size += br.z
            
    # Get some basic info about the files to stack
    with BioReader(input_files[0]['file']) as br:

        # Get the physical z-distance if available, set to physical x if not
        ps_z = br.ps_z
        
        # If the z-distances are undefined, average the x and y together
        if None in ps_z:
            # Get the size and units for x and y
            x_val,x_units = br.ps_x
            y_val,y_units = br.ps_y
            
            # Convert x and y values to the same units and average
            z_val = (x_val*UNITS[x_units] + y_val*UNITS[y_units])/2
            
            # Set z units to the smaller of the units between x and y
            z_units = x_units if UNITS[x_units] < UNITS[y_units] else y_units
            
            # Convert z to the proper unit scale
            z_val /= UNITS[z_units]
            ps_z = (z_val,z_units)
            ProcessManager.log('Could not find physical z-size. Using the average of x & y {}.'.format(ps_z))

        # Hold a reference to the metadata once the file gets closed
        metadata = br.metadata

    has_written = output_path.exists()

    # Create the output file within a context manager
    with BioWriter(output_path,metadata=metadata) as bw:

        # Adjust the dimensions before writing
        bw.z = z_size
        bw.ps_z = ps_z


        # ZIndex tracking for the output file
        zi = file_offset

        # Start stacking
        for file in input_files[file_offset:file_offset+num_files]:

            # Open an image
            with BioReader(file['file']) as br:

                # Open z-layers one at a time
                for z in range(br.z):

                    # Use tiled reading in x&y to conserve memory
                    # At most, [chunk_size, chunk_size] pixels are loaded
                    for xs in range(0,br.x,chunk_size):
                        xe = min([br.x,xs + chunk_size])

                        for ys in range(0,br.y,chunk_size):
                            ye = min([br.y,ys + chunk_size])

                            if has_written:
                                ProcessManager.submit_process(_transfer_thread, br._file_path, bw._file_path, bw.metadata, ys, ye, xs, xe, z, zi)
                            else:
                                bw[ys:ye,xs:xe,zi:zi+1,...] = br[ys:ye,xs:xe,z:z+1,...]
                                has_written = True

                    zi += 1

                # # update the BioWriter in case the ProcessManager found more threads
                # bw.max_workers = ProcessManager._active_threads

        ProcessManager.join_processes()
                
def main(input_dir: pathlib.Path,
         file_pattern: str,
         output_dir: pathlib.Path,
         file_offset: int | None = None
         num_files: int | None = None
         ) -> None:
    
    # create the filepattern object
    fp = filepattern.FilePattern(input_dir,file_pattern)
    
    for files in fp(group_by='z'):
        output_name = fp.output_name(files)
        output_file = output_dir.joinpath(output_name)

        file_offset = 0 if file_offset is None else file_offset
        num_files = len(files) if num_files is None else num_files

        _merge_layers(files,output_file[file_offset:file_offset+num_files],file_offset)
    
    # ProcessManager.join_processes()

if __name__ == "__main__":
    # Initialize the main thread logger
    logger = logging.getLogger('main')
    logger.setLevel(logging.INFO)

    # Setup the Argument parsing
    logger.info('Parsing arguments...')
    parser = argparse.ArgumentParser(prog='main', description='Compile individual tiled tiff images into a single volumetric tiled tiff.')

    parser.add_argument('--inpDir', dest='input_dir', type=str,
                        help='Path to folder with tiled tiff files', required=True)
    parser.add_argument('--outDir', dest='output_dir', type=str,
                        help='The output directory for ome.tif files', required=True)
    parser.add_argument('--filePattern', dest='file_pattern', type=str,
                        help='A filename pattern specifying variables in filenames.', required=True)
    parser.add_argument('--file_offset', dest='offset', type=int,
                        help='The starting file offset. Useful for scattering jobs across nodes', required=True)
    parser.add_argument('--num_files', dest='offset', type=int,
                        help='Number of files to process.', required=True)

    args = parser.parse_args()
    input_dir = pathlib.Path(args.input_dir)
    if input_dir.joinpath("images").is_dir():
        input_dir = input_dir.joinpath("images")
    output_dir = pathlib.Path(args.output_dir)
    file_pattern = args.file_pattern
    file_offset = args.file_offset
    num_files = args.num_files
    logger.info(f'input_dir = {input_dir}')
    logger.info(f'output_dir = {output_dir}')
    logger.info(f'file_pattern = {file_pattern}')
    logger.info(f'max_threads: {ProcessManager.num_processes()}')
    logger.info(f'file_offset: {offset}')
    logger.info(f'num_files: {offset}')
    
    ProcessManager.init_processes('main','stack')

    main(input_dir,
         file_pattern,
         output_dir,
         file_offset,
         num_files)