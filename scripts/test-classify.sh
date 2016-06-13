#!/bin/bash

#if [ $# -ne 1 ]; then
#        echo -e "\nrequires Ontology file name"
#        exit 1
#fi

java -Xms12g -Xmx12g -cp dist/DistEL.jar:lib/jedis-2.6.2.jar knoelab.classification.test.ELClassifierTest $1
