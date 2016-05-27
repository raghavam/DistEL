#!/bin/bash

cd redis-2.6.2/
src/redis-server redis.conf > redis-log &
