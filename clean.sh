docker rm -f $(docker ps -af "label=storage" --format {{.ID}}) && \
docker rm -f $(docker ps -af "label=bully" --format {{.ID}}) && \
./purge_queues.sh $1 
