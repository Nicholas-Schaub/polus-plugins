
FROM ubuntu:18.04
RUN echo 'debconf debconf/frontend select Noninteractive' | debconf-set-selections 
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
#python-sphinx  \
libeigen3-dev  \
#libassimp-dev  \
#libccd-dev     \
#libfcl-dev     \
libboost-regex-dev   \ 
libffi-dev \
  libtiff-tools	\
#libtiff-dev \
libtiff5-dev \
#libtiff5 \
libc6-dev \
libpng-dev \
zlib1g-dev \
#python-libtiff \
libjpeg-dev \
 # liblzma-dev \
 #    liblz-dev \
     g++ \
  #   cvs \
#python-dev \
     python3-dev \
 #    libffi6 
	git

RUN export LANG='en_US.UTF-8' && \
 export LANGUAGE='en_US.UTF-8' \
 export LC_ALL=C.UTF-8  

#RUN apt-get install -y git  \
 #python-setuptools  

# python-pybind11
#RUN apt install -y python-pip
RUN apt install -y python3-pip
RUN apt install -y python3.6-dev
#RUN apt-get install -y python-pytest
RUN apt-get install -y software-properties-common
ENV wd $HOME/ome-files/out
RUN apt-get update && apt-get install \
  -y --no-install-recommends python3 \
	python3-virtualenv 
 #python3-venv &&  \
#python-virtualenv && \
#apt install python3.6-venv && \
# pip3 install virtualenv
RUN  apt update
RUN apt install wget 
RUN echo "deb http://cz.archive.ubuntu.com/ubuntu xenial main universe" > /etc/apt/sources.list
RUN apt-get -f install 
ENV VIRTUAL_ENV=/opt/venv 
RUN python3 -m virtualenv --python=/usr/bin/python3 $VIRTUAL_ENV

RUN . /opt/venv/bin/activate && \
 cd /usr/src/gtest && \
 cmake CMakeLists.txt && \
cmake CMakeLists.txt && \
make  && \
cp *.a /usr/lib 
#ldconfig 



#RUN python3 -m virtualenv --python=/usr/bin/python3 /opt/venv 
WORKDIR  /tmp/  
RUN . /opt/venv/bin/activate && \
 pip3 install pytest \
 numpy && \
#export LC_ALL=C.UTF-8 && \
#pip3 install --upgrade readline  && \
#pip3 install git+git://github.com/pearu/pylibtiff 
git clone https://github.com/pearu/pylibtiff && \
	cd pylibtiff && \
 python3 setup.py install  
#RUN  python3 -m pip install pybind11
#RUN apt-get install libcurl3-gnutls 
#RUN apt-get install -y autoconf
 WORKDIR  /tmp/ 
RUN . /opt/venv/bin/activate && \
pip3 install -U sphinx && \
pip3 install Genshi && \
export CPATH=/tmp/ome-files/out/include && \
 git clone https://github.com/pbsudharsan/ome-files.git && \ 
 cd  ome-files && \
# . /opt/venv/bin/activate && \
  pip3 install pybind11 && \
 pip3 install -r requirements.txt && \
 git submodule update --init --recursive && \
 export LC_ALL=C.UTF-8 && \
 cmake -H. -BRelease -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=./out  && \
 cmake --build Release --target install  && \
 ldconfig -v



ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH://tmp/ome-files/out/lib
ENV CXXFLAGS=$CXXFLAGS://tmp/ome-files/out/include 
WORKDIR /tmp/ome-files/
RUN  . /opt/venv/bin/activate && \
 echo LD_LIBRARY_PATH && \
 #git clean -dfx && \
 cd ome-files-py && \
 pip3 install -r requirements.txt && \
 # export CXXFLAGS= //tmp/ome-files/out/include && \
 python3 setup.py build_ext -I //tmp/ome-files/out/include -L //tmp/ome-files/out/lib -R //tmp/ome-files/out/lib && \
 #python3 setup.py build && \
 pip3 install . && \
# pip3 install -r requirements.txt && \
 python3 setup.py install 
 
WORKDIR /tmp/ome-files/ome-files-py/examples
# RUN cd examples && \
RUN . /opt/venv/bin/activate && \
#LD_LIBRARY_PATH=$LD_LIBRARY_PATH://tmp/ome-files/out/lib && \
export LD_LIBRARY_PATH && \ 
 python3 dump_planes.py ../test/data/multi-channel-4D-series.companion.ome
 	

