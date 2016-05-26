#!/bin/bash

#if [ $# -ne 1 ]; then
#        echo -e "\nrequires Ontology file name"
#        exit 1
#fi

echo -e "Checking the progress of classifier...\n"

java -Xms12g -Xmx12g -cp dist/DistEL.jar:lib/jedis-2.8.1.jar knoelab.classification.test.ELClassifierTest
