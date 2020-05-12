#!/bin/bash

version=$(<VERSION)
 docker build . -t libre:${version}
