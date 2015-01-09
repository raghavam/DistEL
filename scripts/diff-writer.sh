#!/bin/bash

if [ $# -ne 1 ]; then
	echo -e "\nrequires Ontology file name"
	exit 1
fi

#echo -e "Rearranging results...\n"
#java -cp dist/DistEL.jar:lib/jedis-2.6.2.jar knoelab.classification.test.ResultRearranger

echo -e "\nWriting diff results...\n"
java -cp dist/DistEL.jar:lib/jedis-2.6.2.jar knoelab.classification.test.ResultDiffWriter $1
