while read -r line || [[ -n $line ]]; 
do
        scp /home/azureuser/redis-3.2.0/redis.conf "$line":/home/azureuser/redis-3.2.0/
done < $1

