#!/bin/bash

java -Xms28g -Xmx28g -cp dist/DistEL.jar:lib/jedis-2.6.2.jar knoelab.classification.init.AxiomLoader $1 $2 $3 $4
