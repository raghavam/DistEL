#!/bin/bash

echo -e "deleting output & error dirs"
# removing pssh output & error dirs
rm -rf output/
rm -rf error/
rm -rf /data1/DistReasoning/output
rm -rf /data1/DistReasoning/error

echo -e "creating jar...\n"
ant jar

echo -e "copying lib and dist...\n"

nodes=( nimbus2 nimbus3 nimbus4 nimbus5 nimbus6 nimbus7 nimbus8 )
for i in "${nodes[@]}"
do
ssh w030vxm@$i "if [ ! -d ~/DistributedReasoning/curr_approach ]; then mkdir -p ~/DistributedReasoning/curr_approach; fi
if [ ! -d ~/DistributedReasoning/curr_approach/dist ]; then mkdir -p ~/DistributedReasoning/curr_approach/dist; fi
if [ ! -d ~/DistributedReasoning/curr_approach/lib ]; then mkdir -p ~/DistributedReasoning/curr_approach/lib; fi
"
scp lib/jedis-2.1.0.jar w030vxm@$i:~/DistributedReasoning/curr_approach/lib/
scp dist/* w030vxm@$i:~/DistributedReasoning/curr_approach/dist/
done

