#!/bin/bash +vx
LIB_PATH=$"/home/suri/Desktop/Github/eventually_consistent_Key-Value_store/keyValueStore/lib/protobuf-java-3.4.0.jar"
#port
java -classpath bin/classes:$LIB_PATH keyValueStore.client.Client $1 $2
