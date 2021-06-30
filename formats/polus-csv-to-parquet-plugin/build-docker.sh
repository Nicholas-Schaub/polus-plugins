#!/bin/bash

version=$(<VERSION)
docker build . -t labshare/polus-csv-to-parquet-plugin:${version}