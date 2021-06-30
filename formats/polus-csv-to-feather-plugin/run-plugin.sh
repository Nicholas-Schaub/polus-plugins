#!/bin/bash

version=$(<VERSION)
datapath=$(readlink --canonicalize ../data)

# Inputs
inpDir=
filePattern=.+

# Output paths
outDir=/data/path_to_output

# GPU configuration for testing GPU usage in the container
GPUS=all

# Log level, must be one of ERROR, CRITICAL, WARNING, INFO, DEBUG
LOGLEVEL=INFO

docker run --mount type=bind,source=${datapath},target=/data/ \
            --user $(id -u):$(id -g) \
            --gpus ${GPUS} \
            --env POLUS_LOG=${LOGLEVEL} \
            labshare/polus-csv-to-feather-plugin:${version} \
            --inpDir ${inpDir} \
            --filePattern ${filePattern} \
            --outDir ${outDir} 
            