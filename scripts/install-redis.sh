#!/bin/bash

wget https://github.com/antirez/redis/archive/2.6.2.tar.gz
mv 2.6.2.tar.gz redis-2.6.2.tar.gz
tar -xvzf redis-2.6.2.tar.gz
cd redis-2.6.2/
#sudo apt-get install make
#sudo apt-get --assume-yes install gcc
make distclean
make
