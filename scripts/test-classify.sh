#!/bin/bash

if [ $# -ne 1 ]; then
        echo -e "\nrequires Ontology file name"
        exit 1
fi

echo -e "Checking the progress of classifier...\n"

java -Xms6g -Xmx6g -cp dist/DistEL.jar:lib/jedis-2.0.0-build.jar knoelab.classification.test.ELClassifierTest $1
