#!/bin/bash

#echo "RUN DOCKER CONTAINERS" &&\
#        python3 main.py &&\
#        sleep 2 &&\
#        echo "KILL_LEADER"&&\
#        docker rm -rf cs-2 sn-2 &&\
#        echo "COLLECT LOGS" &&\
#        sleep 1 &&\
readonly MAX_ITER=$1
readonly MAX_PEERS=$2
readonly LEADER_INDEX=$(($MAX_PEERS-1))
readonly SINK_FOLDER=$HOME/Programming/Python/cinvestav/src/cinvestav-tssd-bully/data

for i in $(seq 0 $MAX_ITER)
do
        echo "CREATE FOLDER experiment_$i at $SINK_FOLDER"
        mkdir $SINK_FOLDER/experiment_$i
        echo "RUN DOCKER CONTAINER"
        python3 main.py $MAX_PEERS
        sleep 5
        echo "KILL_LEADER cs-$LEADER_INDEX / sn-$LEADER_INDEX"
        docker rm -rf cs-$LEADER_INDEX sn-$LEADER_INDEX
        sleep 4
        echo "MOVE LOG FILES to experiment_$i"
        mv ~/Documents/test/storage/logs/*.txt $SINK_FOLDER/experiment_$i
        echo "KILL STORAGE AND BULLY NODES"
        docker rm -rf $(docker ps -f "label=storage" --format {{.ID}})
        docker rm -rf $(docker ps -f "label=bully" --format {{.ID}})
        echo "___________________________________________________________________"
done 

