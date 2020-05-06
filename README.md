# **OME Files  (Alternative to Java Backend for bfio)
 
 This tool is an alternative to the [Bioformats java library](https://www.openmicroscopy.org/bio-formats/) which our pipeline is based on.   It was a proof of concept only, and  works only for simple image reading and writing into numpy array.
 
 **Note: This implementation    is based on   [ome-files](https://gitlab.com/codelibre/ome) . The dockerfile still has issues which are yet  to be fixed.
  
  
  ## Universal Container Components
  [Implementation of ome-files](https://github.com/pbsudharsan/ome-files)
  Cmake
  sphinx
  pybind11
  xerces-c
  boost
  xalan-c
  libtiff
  pylibtiff
  
  
  ## Containers
  /buster
  This container is built on a  version of Debian Buster with python3.6 preinstalled.
  
  
