#!/bin/bash

#ant jar
echo "deleting keys..."
java -cp dist/DistEL.jar:lib/jedis-2.8.1.jar knoelab.classification.misc.DeleteKeys distributed
