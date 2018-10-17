#!/bin/bash +vx
LIB_PATH=$"/home/yaoliu/src_code/local/lib/usr/local/lib/libthrift-0.10.0.jar:/home/yaoliu/src_code/local/lib/usr/local/lib/slf4j-log4j12-1.7.12.jar:/home/yaoliu/src_code/local/lib/usr/local/lib/slf4j-api-1.7.12.jar:/home/yaoliu/src_code/local/lib/usr/local/lib/log4j-1.2.17.jar"
#simple/secure ip port
java -classpath bin/client_classes:$LIB_PATH Client $1 $2 $3 $4
