
FROM python:3.6.10-buster
# Installing  depoendent libraries (Few of them  may not be required)
RUN apt-get -y update  && apt-get install -y \
build-essential \
cmake \
libboost-all-dev \
libxalan-c-dev \
libqt5xmlpatterns5-dev \
libfmt-dev \
doxygen \
libgtest-dev \
libqt5svg5-dev \
libglm-dev \
python-sphinx  \
libeigen3-dev  \
libassimp-dev  \
libccd-dev     \
libfcl-dev     \
libboost-regex-dev   \ 
libffi-dev \
  libtiff-tools	\
libtiff-dev \
libtiff5-dev \
libtiff5 \
libc6-dev \
libpng-dev \
zlib1g-dev \
python-libtiff \
libjpeg-dev \
  liblzma-dev \
     liblz-dev \
     g++ \
     python3-dev \
     libffi6

RUN apt-get install -y software-properties-common \
 python3.7-dev  \
 mesa-utils    \
 python-pytest \
 python3-jinja2 \
 python3-pygments
RUN  apt install -y openjdk-11-jdk
RUN apt-get install openjdk-11-jre

RUN  apt  -y  update 

RUN  apt-get install -y python3-pip && \
 apt-get install -y python3-setuptools \ 
python3-distutils 

RUN apt-get install -y python3-pytest
RUN  apt update  
RUN pip3 install virtualenv 
RUN export LANG='en_US.UTF-8'
RUN export LANGUAGE='en_US.UTF-8'

RUN apt-get install -y texlive-xetex \
 libxerces-c-doc
RUN apt-get install -y
WORKDIR /tmp/
# Setting up Virtual Environment
RUN python -m pip install --upgrade pip
RUN python3 -m virtualenv --python=/usr/bin/python3 /opt/venv

# Building  pybind11
RUN . /opt/venv/bin/activate  && \
 pip3 install pytest \
  Sphinx \
 numpy  \
 bitarray && \
#export LC_ALL=C.UTF-8 && \
 git clone https://github.com/pybind/pybind11.git && \ 
cd pybind11 && \
mkdir build && \
cd build  && \
cmake ../ && \
make   && \
 make install && \
 ldconfig -v

WORKDIR ./pybind11  
RUN . /opt/venv/bin/activate && \
#export LC_ALL=C.UTF-8 && \
python3 setup.py install  


#RUN apt-get install -y git build-essential automake autoconf libtool* make
WORKDIR /tmp/ 
RUN . /opt/venv/bin/activate && \
wget http://mirror.ox.ac.uk/sites/rsync.apache.org/xerces/c/3/sources/xerces-c-3.2.3.tar.gz  && \
tar -zxvf xerces-c-3.2.3.tar.gz  && \
 cd     xerces-c-3.2.3 && \
mkdir build && \
cd build && \
cmake .. && \
make -j4 && \
make install && \
ldconfig


RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    xsltproc \
    xalan 
WORKDIR /tmp/
RUN . /opt/venv/bin/activate  && \
git clone https://github.com/apache/xalan-c.git && \
cd xalan-c && \
cmake . && \
make -j4 && \
make install && \
ldconfig -v

# Setting up Environment variable path
ENV wd $HOME/ome-files/out

# installing Libtiff
RUN apt-get -y install  
WORKDIR /tmp/ 
RUN . /opt/venv/bin/activate && \
 wget http://download.osgeo.org/libtiff/tiff-4.0.10.tar.gz && \
tar -zxvf tiff-4.0.10.tar.gz && \
cd tiff-4.0.10 && \
./configure && \
make . && \
make install  && \
 ldconfig -v

# Setting up pylibtiff
WORKDIR /tmp/  
RUN . /opt/venv/bin/activate && \ 
pip3 install wheel && \
export LC_ALL=C.UTF-8 && \
 git clone https://github.com/pearu/pylibtiff && \
	cd pylibtiff && \
 python3 setup.py install && \
 ldconfig -v
# installing pybind11
RUN  python3 -m pip install pybind11

#BUilding the repo
WORKDIR /tmp/   
RUN . /opt/venv/bin/activate && \
 export CPATH=/tmp/ome-files/out/include && \
 git clone https://github.com/pbsudharsan/ome-files.git && \ 
 cd  ome-files && \
 pip3 install -r requirements.txt && \
 git submodule update --init --recursive && \
 cmake -H. -BRelease -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=./out  && \
 cmake --build Release --target install  && \
 ldconfig -v




WORKDIR /tmp/ome-files
RUN apt-get install libcurl3-gnutls


RUN . /opt/venv/bin/activate && \
 cd ome-files-py && \
 pip3 install -r requirements.txt && \
 export LIBRARY_PATH=$LD_LIBRARY_PATH:/tmp/ome-files/out/lib && \


 #export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/tmp/ome-files/out/lib && \
# export CPATH=/usr/local/include -L/usr/local/lib stdlib=libc++  && \
# export CPATH=/tmp/ome-files/out/include && \
 python3 setup.py build_ext -I /tmp/ome-files/out/include -L /tmp/ome-files/out/lib -R /tmp/ome-files/out/lib  && \
 pip3 install . && \
 python3 setup.py build && \
 python3 setup.py install





WORKDIR /tmp/ome-files/ome-files-py/examples
RUN . /opt/venv/bin/activate && \
 pip3 install bitarray && \
#ln -s /usr/lib/x86_64-linux-gnu/libtiff.so.5	 /usr/lib/x86_64-linux-gnu/libtiff.so.4 && \
 python3 dump_planes.py ../test/data/multi-channel-4D-series.companion.ome
 	

