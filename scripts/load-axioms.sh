#!/bin/bash

java -Xms12g -Xmx12g -cp dist/DistEL.jar:lib/jedis-2.8.1.jar knoelab.classification.init.AxiomLoader $1 $2 $3 $4
