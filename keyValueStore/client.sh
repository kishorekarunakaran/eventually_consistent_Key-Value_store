#!/bin/bash +vx
LIB_PATH=$"/home/phao3/protobuf/protobuf-3.4.0/java/core/target/protobuf.jar"
#port
java -classpath bin/classes:$LIB_PATH keyValueStore.client.Client $1 $2
