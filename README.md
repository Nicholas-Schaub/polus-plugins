# OME Files  ( C++ Alternative to Java Backend for BFIO)
 
 This tool can be used as  an alternative to  [Bioformats java library](https://www.openmicroscopy.org/bio-formats/) which our pipeline is based on.   It was a proof of concept only, and  works only for simple image reading and writing into numpy array.
 It's advantagious  to use  C++ over Java  because of faster compile time and reduced size of docker container.
 **Note**: This implementation is based on [ome-files](https://gitlab.com/codelibre/ome) . The dockerfile has issues which are yet  to be fixed.
  
  
  ##  Container Components
  [Implementation of ome-files](https://github.com/pbsudharsan/ome-files) (Forked from orginal repo) \
  Cmake \
  sphinx \
  pybind11 \
  xerces-c \
  boost \
  xalan-c \
  libtiff \
  pylibtiff \
  libtiff
  
  
  ## Containers
  [/buster](https://github.com/pbsudharsan/polus-plugins/tree/bfio/backend/Buster) \
  This container is built on a  version of Debian Buster with python3.6 preinstalled.
  
  
