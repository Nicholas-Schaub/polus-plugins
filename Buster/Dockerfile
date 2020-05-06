
FROM python:3.6.10-buster

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

RUN apt-get install -y software-properties-common
RUN apt-get install -y python3.7-dev
RUN apt-get install -y mesa-utils
RUN apt-get install -y python-pytest \ 
 python3-jinja2 \
 python3-pygments
RUN  apt install -y openjdk-11-jdk
RUN apt-get install openjdk-11-jre
#RUN \
 # add-apt-repository -r ppa:webupd8team/java && \
 # apt-get update && \
  #echo oracle-java8-installer shared/accepted-oracle-license-v1-1 select true | debconf-set-selections && \
#  add-apt-repository -y ppa:webupd8team/java && \
#  apt-get update && \
  #apt-get install -y oracle-java8-installer --allow-unauthenticated && \
  #rm -rf /var/lib/apt/lists/* && \
  #rm -rf /var/cache/oracle-jdk8-installer
 #   apt-get install -y openjdk-8-jdk && \
 #   apt-get install -y ant && \
#    apt-get clean

RUN LD_LIBRARY_PATH=/usr/local/lib
#RUN apt-get update && \
#    apt-get install ca-certificates-java && \
#    apt-get clean && \
 #   update-ca-certificates -f;
#add-apt-repository ppa:linuxuprising/java && \
#RUN echo "deb http://ppa.launchpad.net/linuxuprising/java/ubuntu bionic main" |  tee /etc/apt/#sources.list.d/linuxuprising-java.list && \
#apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 73C3DB2A

RUN  apt  -y  update 
 #      apt install -y oracle-java11-installer
#RUN apt-get -y install  
RUN  apt-get install -y python3-pip && \
 apt-get install -y python3-setuptools \ 
python3-distutils 
#linux-headers-amd64
#RUN apt-get install python-pybind11
RUN apt-get install -y python3-pytest
#RUN python --version
RUN  apt update  
RUN pip3 install virtualenv 
#RUN apt-get update && \

RUN apt-get install -y texlive-xetex \
 libxerces-c-doc
RUN apt-get install -y
WORKDIR /tmp/

RUN python -m pip install --upgrade pip
RUN python3 -m virtualenv --python=/usr/bin/python3 /opt/venv 
RUN . /opt/venv/bin/activate  && \
 pip3 install pytest \
  Sphinx \
 numpy  \
 bitarray && \
export LC_ALL=C.UTF-8 && \
 git clone https://github.com/pybind/pybind11.git && \ 
cd pybind11 && \
#mkdir build  && \
#cd build     && \
#cmake .. -DCMAKE_BUILD_TYPE:STRING="MinSizeRel" -DEigen3_DIR:PATH="Eigen3_DIR-NOTFOUND" -DCMAKE_INSTALL_PREFIX:PATH="/usr/local" -DPYBIND11_CMAKECONFIG_INSTALL_DIR:STRING="share/cmake/pybind11" -#DPYBIND11_CPP_STANDARD:STRING="-std=c++14" -DCATCH_INCLUDE_DIR:PATH="CATCH_INCLUDE_DIR-NOTFOUND"   && \
#make -j4    && \
#make install  
mkdir build && \
cd build  && \
cmake ../ && \
make   && \
 make install && \
 ldconfig -v

WORKDIR ./pybind11  
RUN . /opt/venv/bin/activate && \
export LC_ALL=C.UTF-8 && \
python3 setup.py install  

RUN export LANG='en_US.UTF-8'
RUN export LANGUAGE='en_US.UTF-8'
RUN apt-get install -y git build-essential automake autoconf libtool* make
WORKDIR /tmp/ 
RUN . /opt/venv/bin/activate && \
wget http://mirror.ox.ac.uk/sites/rsync.apache.org/xerces/c/3/sources/xerces-c-3.2.3.tar.gz  && \
tar -zxvf xerces-c-3.2.3.tar.gz  && \
 cd     xerces-c-3.2.3 && \
mkdir build && \
cd build && \
cmake .. && \
make -j8 && \
make install && \
#ldconfig
#ENV XERCES_VERSION Xerces-C_3_2_0
#git clone https://github.com/apache/xerces-c.git --branch ${XERCES_VERSION} && \
#     cd xerces-c && \
#./reconf && \
#./configure && \
#make && \
#make install && \
# && make check\
  #  && rm -rf /xerces-c\ 
ldconfig
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    xsltproc \
    xalan 
WORKDIR /tmp/ && \
RUN . /opt/venv/bin/activate  && \
git clone https://github.com/apache/xalan-c.git && \
cd xalan-c && \
cmake .. && \
make -j4 && \
make install && \
ldconfig -v






ENV wd $HOME/ome-files/out
#RUN apt-get update && apt-get install \
#  -y --no-install-recommends python3 \
#	python3-virtualenv 

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

WORKDIR /tmp/  
RUN . /opt/venv/bin/activate && \ 
pip3 install wheel && \
export LC_ALL=C.UTF-8 && \
# pip3 install git+git://github.com/pearu/pylibtiff 
 git clone https://github.com/pearu/pylibtiff && \
	cd pylibtiff && \
 python3 setup.py install && \
 ldconfig -v
#RUN apt-get install -y python-pybind11
RUN  python3 -m pip install pybind11
WORKDIR /tmp/   
RUN . /opt/venv/bin/activate && \
 export CPATH=/tmp/ome-files/out/include && \
 git clone https://github.com/pbsudharsan/ome-files.git && \ 
 cd  ome-files && \
 #pip3 install -r requirements.txt && \
 git submodule update --init --recursive && \
 cmake -H. -BRelease -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX=./out  && \
 cmake --build Release --target install  && \
 ldconfig -v
# git clean -dfx 
#RUN apt-get install -y texlive-xetex
#RUN apt-get update --fix-missing && \
 #   apt-get install -y software-properties-common && \
#    rm -rf /var/lib/apt/lists/*


WORKDIR /tmp/ome-files 

RUN apt-get install libcurl3-gnutls 
#libcrypto++9

RUN which cmake

RUN . /opt/venv/bin/activate && \
 cd ome-files-py && \
 #pip install -r requirements.txt && \
 #pip3 install -r requirements.txt && \
 export LIBRARY_PATH=$LD_LIBRARY_PATH:/tmp/ome-files/out/lib && \
 #export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/tmp/ome-files/out/lib && \
# export CPATH=/usr/local/include -L/usr/local/lib stdlib=libc++  && \
# export CPATH=/tmp/ome-files/out/include && \
 python3 setup.py build_ext -I /tmp/ome-files/out/include -L /tmp/ome-files/out/lib -R /tmp/ome-files/out/lib  && \
 pip3 install . && \
 python3 setup.py build && \
 python3 setup.py install
 #ldconfig -v






WORKDIR /tmp/ome-files/ome-files-py/examples
RUN . /opt/venv/bin/activate && \
 pip3 install bitarray && \
#ln -s /usr/lib/x86_64-linux-gnu/libtiff.so.5	 /usr/lib/x86_64-linux-gnu/libtiff.so.4 && \
 python3 dump_planes.py ../test/data/multi-channel-4D-series.companion.ome
 	

