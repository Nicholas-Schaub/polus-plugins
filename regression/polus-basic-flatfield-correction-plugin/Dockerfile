FROM tensorflow/tensorflow:2.9.1-gpu
# FROM nvidia/cuda:11.2.1-cudnn8-runtime

# environment variables defined in labshare/polus-bfio-util
ENV EXEC_DIR="/opt/executables"
ENV DATA_DIR="/data"
ENV POLUS_EXT=".ome.tif"

# Change to WARNING for fewer logs, and DEBUG for debugging
ENV POLUS_LOG="INFO"
ENV BASIC_DCT_BACKEND="JAX"

RUN apt-get update && apt-get install software-properties-common wget -y
RUN add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update & \
    apt-get install python3.9 -y

RUN wget https://bootstrap.pypa.io/get-pip.py && \
    python3.9 get-pip.py && \
    python3.9 -m pip install pip --upgrade

# Work directory defined in the base container
WORKDIR ${EXEC_DIR}

COPY VERSION ${EXEC_DIR}
COPY src ${EXEC_DIR}/

RUN python3.9 -m pip install "jax[cuda]" -f https://storage.googleapis.com/jax-releases/jax_cuda_releases.html
# RUN pip install jax[cuda] jaxlib==0.1.65+cuda112 -f https://storage.googleapis.com/jax-releases/jax_cuda_releases.html
RUN python3.9 -m pip install -r ${EXEC_DIR}/requirements.txt --no-cache-dir

# Default command. Additional arguments are provided through the command line
ENTRYPOINT ["python3.9", "main.py"]
