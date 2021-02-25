FROM python:3.9-slim

ARG EXEC_DIR="/opt/executables"

COPY . ${EXEC_DIR}

WORKDIR ${EXEC_DIR}
RUN pip3 install .

ENTRYPOINT [ "python3", "-m", "unittest", "tests.docker_test" ]