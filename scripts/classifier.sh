#!/bin/bash

cd DistributedReasoning/curr_approach
echo -e "\nStarting ELClassifier....\n"
java -Xms8g -Xmx8g -cp dist/DistEL.jar:lib/jedis-2.6.2.jar knoelab.classification.ELClassifier
