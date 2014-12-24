#!/bin/bash

if [ $# -ne 1 ]; then
	echo -e "\nProvide folder path containing ontologies"
	exit 1
fi

echo -e "Loading and classifying base traffic ontologies (Time, Travel, Spatio)" > log-classify
java -Xms4g -Xmx4g -cp dist/DistEL.jar:lib/jedis-2.1.0.jar knoelab.classification.init.AxiomLoader ../trips-data/norm-merged-spatio-temporal-travel-distel.owl true true false >> log-classify
(time ./scripts/classify-all.sh) &>> log-classify 

echo -e "-------------------------------------" >> log-classify

i=1;
for f in "$@"/*.owl 
do
	echo -e "$i    file: $f" >> log-classify
	java -Xms4g -Xmx4g -cp dist/DistEL.jar:lib/jedis-2.1.0.jar knoelab.classification.init.AxiomLoader "$f" true true true >> log-classify
	(time ./scripts/classify-all.sh) &>> log-classify
	echo -e "-------------------------------------" >> log-classify
	i=$((i+1));
done
