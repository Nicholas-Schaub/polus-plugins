# OME Files  ( C++ Alternative to Java Backend for BFIO)
 
 This tool can be used as  an alternative to  [Bioformats java library](https://www.openmicroscopy.org/bio-formats/) which our pipeline is based on.It was a proof of concept. So it works only for simple image reading and writing into numpy array.
 It's advantagious  to use  C++ over Java  because of faster compile time and reduced size of docker container. \
 **Note**: This is a implementation of [ome-files](https://gitlab.com/codelibre/ome) .Check [Fork](https://github.com/pbsudharsan/ome-files)  for additional  information on the ome repo. 
 
  
  ##  Container Components
  [Implementation of ome-files](https://github.com/pbsudharsan/ome-files) (Forked from orginal repo) \
  Cmake \
  sphinx \
  pybind11 \
  xerces-c \
  boost \
  xalan-c \
  libtiff \
  pylibtiff 
  
  ## Containers
  [/Ubuntu](https://github.com/pbsudharsan/polus-plugins/tree/bfio/backend/Ubuntu) \
  This folder contains 2 dockerfiles .Dockerfile-compile with Ubuntu:18.04 as base image installs the dependencies and installs the build. Dockerfile-run runs the  test python script with Dockerfile-compile as base image.
  ## Instaling Ome-C++ as a python package
  Check [/Ubuntu/Dockerfile-compile](https://github.com/pbsudharsan/polus-plugins/blob/bfio/backend/utils/polus-bfio-util-C%2B%2B/Ubuntu/Dockerfile-compile)
  for installing .
  
  ##  Building the containers  
   Make the build.sh script executable and run  \
   ```bash
   ./build.sh
  ```
 ## To Do 
 1. Make the docker build multistage.
 2. Remove hard coded paths in the dockerfile.
  
