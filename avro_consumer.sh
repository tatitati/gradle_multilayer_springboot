#!/bin/bash

docker run  -it --rm  --net=host  confluentinc/cp-schema-registry:3.1.1 bash "$@"