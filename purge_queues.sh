#!/bin/bash
readonly LEADER_INDEX=$(($1-1))
for j in $(seq 0 $LEADER_INDEX) 
do
        docker exec -it r00 rabbitmqctl purge_queue pool-xxxx-cs-$j
        docker exec -it r00 rabbitmqctl purge_queue pool-xxxx-sn-$j
done 
