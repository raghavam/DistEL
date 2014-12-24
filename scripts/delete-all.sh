#!/bin/bash

#ant jar
echo "deleting keys..."
java -cp dist/DistEL.jar:lib/jedis-2.1.0.jar knoelab.classification.misc.DeleteKeys distributed
