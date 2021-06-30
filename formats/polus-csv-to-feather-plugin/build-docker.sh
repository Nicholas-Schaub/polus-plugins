#!/bin/bash

version=$(<VERSION)
docker build . -t labshare/polus-csv-to-feather-plugin:${version}