#!/bin/bash

java -Xms6g -Xmx6g -cp dist/DistEL.jar:lib/jedis-2.1.0.jar knoelab.classification.init.AxiomLoader $1 $2 $3 $4
