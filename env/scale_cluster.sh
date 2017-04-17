#!/usr/bin/env bash

if [ "$1" != "" ]; then
    echo " Running Cassandra cluster with $1 node"
    docker-compose -p cluster scale cassandra-node=$1
else
    echo "Node size not specified. Running Cassandra cluster with 1 node"
    docker-compose -p cluster scale cassandra-node=1
fi