DistEL
======

DistEL is a distributed classifier for EL+ ontologies. It runs on a cluster of machines. DistEL is short name for Distributed EL Classifier. 
DistEL in german, translates to thistle. Implementation is by Raghava Mutharaju (mutharaju.2@wright.edu) under the supervision of 
Prof. Pascal Hitzler (http://knoesis.org/pascal) along with many helpful discussions involving Prof. Prabhaker Mateti (http://www.cs.wright.edu/~pmateti/PM/index.html).


## Dependencies

DistEL depends on the following software 

1. Redis (http://redis.io)
2. pssh (https://code.google.com/p/parallel-ssh)
3. Java, version 1.6 or above
4. ant (http://ant.apache.org)

Please download and install all of them. Redis needs to be installed on all the machines in the cluster that you plan to make use of. 
Add the executables to PATH environment variable. Except Redis and Java, all the other software can be installed only on one machine, which we call
the "head" machine. This is the machine from which the distributed computation would be initiated.  


## Instructions to classify ontologies

1. Download DistEL source code and compile using the command, "ant jar". Lets call this machine as the "head".
2. Start Redis on all the machines in the cluster. 
3. In ShardInfo.properties, do the following changes (sample assignment is already provided in the properties file)
	- assign Redis instances (host name/IP:port format) to rules. 
	- provide the hostname:port of the results node.
	- change shard.count to the total number of machines assigned to rules.
4. Enable passwordless ssh from the "head" machine to all the machines in the cluster.
5. pssh needs a hosts.txt file. For each machine, add username@machine-name per line in the file. 
6. The folder names in the shell script files (in scripts folder) should be changed appropriately to reflect your folder structure.
7. Run the following scripts in the order mentioned here.
	- init.sh: This file builds a jar from source and copies the jar file to all the machines of the cluster
	- load-axioms.sh: Takes a single argument, which is the name of the ontology file. Ontology should be normalized. 
	If the ontology is not normalized, modify the script and pass "false" to AxiomLoader class in the script file.
	- classify-all.sh: After loading axioms in the cluster, run this script to classify the axioms in the database of all the machines.
8. Results would be stored on the result node indicated in ShardInfo.properties file.
9. Results can be verified against other popular reasoners such as ELK, jCEL or Pellet using the shell script file test-classify.sh 	


Note that DistEL can also be run on a single machine which has sufficient RAM. But a minimum of 7 Redis instances need to run on the single machine. 