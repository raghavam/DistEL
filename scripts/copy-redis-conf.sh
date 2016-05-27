while read -r line || [[ -n $line ]]; 
do
        scp /home/azureuser/redis-2.6.2/redis.conf "$line":/home/azureuser/redis-2.6.2/
done < $1

