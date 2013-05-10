#!/bin/bash

if [ $# -ne 1 ]; then
	echo -e "\nrequires Ontology file name"
	exit 1
fi

java -Xms6g -Xmx6g -cp dist/DistributedELCompletionRules.jar:lib/jedis-2.1.0.jar knoelab.classification.init.AxiomLoader $1 true
