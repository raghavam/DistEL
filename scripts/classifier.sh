#!/bin/bash

cd DistributedReasoning/curr_approach
echo -e "\nStarting ELClassifier....\n"
java -Xms8g -Xmx8g -cp dist/DistEL.jar:lib/jedis-2.1.0.jar knoelab.classification.ELClassifier
