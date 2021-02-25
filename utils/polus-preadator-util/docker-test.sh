#!/bin/bash

version=$(<VERSION)
docker build . -f ./tests/Dockerfile -t preadator:${version}
docker run --cpuset-cpus=0 preadator:${version}