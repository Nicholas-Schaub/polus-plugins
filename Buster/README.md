**Note** : The dockerfile is being worked on and it may be contain packages that may be unecessary.

# DockeFile 
The dockerfile is built from a base python 3.6 buster image. 





# FAQ
Why virtual environment is needed when base image has python? \
One of dependent packages was installing python3.7 . So this issue was solved by using virtual environments.


Why build packages from source? \
It was necessary to  build the headers from cmake as well as  install python packages.Using virtual environment makes sure that same python version is used throughout the  dockerfile.

How to overcome "Unable to find libtiff4" error? \
Libitff4 is part of the Libtiff package installed when building [libtiff](https://github.com/pbsudharsan/polus-plugins/blob/bfio/backend/Buster/Dockerfile#L118) from source helps solve this issue.

How to solve unable to fing pybind11 header?
Apart from pip installing the package building  nad installing the source files will solve this issue.

Why openjdk-11-jdk  openjdk-11-jre is  necessary? \
Java was installed because it was  one of the GTest cases for Libtiff


