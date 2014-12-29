#!/bin/sh
BASE_DIR=$(dirname $0)
LIB_DIR=$BASE_DIR/../lib
#PROFILE_ARGS="-javaagent:$HOME/jars/testing/jip-1.2/profile/profile.jar -Dprofile.properties=$HOME/jars/testing/jip-1.2/profile/profile.properties"

for i in $(ls $LIB_DIR); do
  export LOCAL_CP="$LOCAL_CP:$LIB_DIR/$i"
done

java -server -Xmx1024m -cp $LOCAL_CP $PROFILE_ARGS net.kmbnw.sort.MergeSort $@
