#!/bin/bash

#ant jar
echo "deleting keys..."
#echo "Don't use this now for sparql stuff..."
java -cp dist/DistributedELCompletionRules.jar:lib/jedis-2.1.0.jar knoelab.classification.misc.DeleteKeys distributed
