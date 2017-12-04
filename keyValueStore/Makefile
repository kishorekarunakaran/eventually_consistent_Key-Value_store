LIB_PATH=/home/phao3/protobuf/protobuf-3.4.0/java/core/target/protobuf.jar

all: clean
	mkdir bin
	mkdir bin/classes
	javac -classpath $(LIB_PATH) -d bin/classes/ src/keyValueStore/client/* src/keyValueStore/server/* src/keyValueStore/util/* src/keyValueStore/keyValue/*

clean:
	rm -rf bin/ log/

