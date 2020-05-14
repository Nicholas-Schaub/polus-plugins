#!/bin/bash

version=$(<VERSION)
 docker build .  -f ./Ubuntu/Dockerfile-compile -t libre-compile:${version}  
 docker build .  -f ./Ubuntu/Dockerfile-run -t libre-run:${version} --build-arg version=${version}
