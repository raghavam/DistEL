#!/bin/bash

if [ $# -ne 1 ]; then
	echo -e "\nrequires Ontology file name"
	exit 1
fi

echo -e "deleting output & error dirs"
# removing pssh output & error dirs
rm -rf output/
rm -rf error/

#echo "Building jar..."
#ant jar
echo -e "\ncopying jar to all the nodes...\n"
nodes=( nimbus2 nimbus3 nimbus4 nimbus5 )

for i in "${nodes[@]}"
do
	rsync -ae ssh dist/DistEL.jar w030vxm@$i:~/DistributedReasoning/DistributedELCompletionRules/dist/	
done

#echo -e "\nDeleting keys from shards...."
#java -cp dist/DistEL.jar:lib/jedis-2.0.0-build.jar knoelab.classification.misc.DeleteKeys distributed
#echo -e "\nLoading axioms...."
#java -cp dist/DistEL.jar:lib/jedis-2.0.0-build.jar:lib/owlapi-bin.jar knoelab.classification.init.AxiomLoader ../TestOntologies/$1 false
echo -e "\nClassifier starts....\n"
pssh -h hosts.txt -t 0 -o output -e error -I < classifier.sh
