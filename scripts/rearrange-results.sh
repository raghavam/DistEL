#!/bin/bash

#if [ $# -ne 1 ]; then
#	echo -e "\nrequires Ontology file name"
#	exit 1
#fi

echo -e "Rearranging results...\n"
java -cp dist/DistributedELCompletionRules.jar:lib/jedis-2.1.0.jar knoelab.classification.test.ResultRearranger

#echo -e "\nWriting diff results...\n"
#java -cp dist/DistributedELCompletionRules.jar:lib/jedis-2.0.0-build.jar knoelab.classification.test.ResultDiffWriter $1
