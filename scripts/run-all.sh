#!/bin/bash

if [ $# -ne 3 ]; then
	echo -e "requires \n\t 
				1) ontology file path, \t 
				2) log path following and \t 
				3) #times to run"
	exit 1
fi

for (( c=1; c<=$3; c++ ))
do

# deleting DB contents twice just in case
scripts/delete-all.sh
scripts/delete-all.sh

# copy DistEL jar, lib, properties file to all nodes
scripts/init.sh nodes.txt azureuser ShardInfo.properties

# load axioms
echo "loading time ..." > $2/log$c.txt
scripts/load-axioms.sh $1 true false false >> $2/log$c.txt

# classify the ontology
echo "classification time ..." >> $2/log$c.txt
(time scripts/classify-all.sh) >> $2/log$c.txt 2>&1

# move output and error directories to log directory
cp -ar output/ $2/output$c
cp -ar error/ $2/error$c

done