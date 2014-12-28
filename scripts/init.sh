#!/bin/bash

if [ $# -ne 3 ]; then
        echo -e "\nrequires nodes file, user name, properties file as input\n"
	echo -e "on AWS EC2, to get list of nodes use, ec2dnic -O <Access ID> -W <Secret Key> -H | cut -f13 | tail -n +2 | grep -v '^$'"
        exit 1
fi

echo -e "deleting output & error dirs"
# removing pssh output & error dirs
rm -rf output/
rm -rf error/

echo -e "creating jar...\n"
ant jar

echo -e "creating hosts.txt for use with pssh\n"

if [ -f hosts.txt ];
then
        rm hosts.txt;
fi

i=0;
while read -r line || [[ -n $line ]]; 
do
        echo "$2@$line" >> hosts.txt;
        nodes[i]=$line;
	i=$i+1;
        if [ -z "$nodesPortCSV" ];
                then nodesPortCSV="$line:6379";
                else nodesPortCSV="$nodesPortCSV,$line:6379";
        fi
done < $1

echo -e "updating NODES_LIST entry in $3\n"

sed -i 's/^[ \t]*NODES_LIST[ \t]*=\([ \t]*.*\)$/NODES_LIST = '${nodesPortCSV}'/' $3

echo -e "copying lib and dist to all the nodes\n"

for i in "${nodes[@]}"
do
ssh "$2@$i" "if [ ! -d ~/DistributedReasoning/curr_approach ]; then mkdir -p ~/DistributedReasoning/curr_approach; fi
if [ ! -d ~/DistributedReasoning/curr_approach/dist ]; then mkdir -p ~/DistributedReasoning/curr_approach/dist; fi
if [ ! -d ~/DistributedReasoning/curr_approach/lib ]; then mkdir -p ~/DistributedReasoning/curr_approach/lib; fi
"
scp lib/jedis-2.5.1.jar "$2@$i":~/DistributedReasoning/curr_approach/lib/
scp dist/* "$2@$i":~/DistributedReasoning/curr_approach/dist/
scp ShardInfo.properties "$2@$i":~/DistributedReasoning/curr_approach/
done
