#!/bin/bash

if [ $# -ne 2 ]; then
	echo -e "\n\t requires ontology file path and \n\t log path"
	exit 1
fi

# deleting DB contents twice just in case
scripts/delete-all.sh
scripts/delete-all.sh

# copy DistEL jar, lib, properties file to all nodes
scripts/init.sh nodes.txt azureuser ShardInfo.properties

# load axioms
scripts/load-axioms.sh /home/azureuser/ontologies/$1 true false false

# classify the ontology
time scripts/classify-all.sh > /home/azureuser/logs/expt_stats/@2/log.txt

# move output and error directories to log directory
cp -ar output/ /home/azureuser/logs/expt_stats/@2/
cp -ar error/ /home/azureuser/logs/expt_stats/@2/