#!/bin/bash

#ant jar
echo "deleting keys..."
java -cp dist/DistEL.jar:lib/jedis-2.6.2.jar knoelab.classification.misc.DeleteKeys distributed
