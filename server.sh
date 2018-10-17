#!/bin/bash +vx
LIB_PATH=$"/home/yaoliu/src_code/local/lib/usr/local/lib/libthrift-0.10.0.jar:/home/yaoliu/src_code/local/lib/usr/local/lib/slf4j-log4j12-1.7.12.jar:/home/yaoliu/src_code/local/lib/usr/local/lib/slf4j-api-1.7.12.jar:/home/yaoliu/src_code/local/lib/usr/local/lib/log4j-1.2.17.jar"
#port
java -classpath bin/server_classes:$LIB_PATH Server $1
