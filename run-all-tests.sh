#! /bin/bash

dir=$(pwd)

cd $dir/rocketmq-spring && ./gradlew test &&
    cd $dir/rocketmq-springboot && ./gradlew test
