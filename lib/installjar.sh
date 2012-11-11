#!/bin/sh

mvn install:install-file -Dfile=./cassandra-jdbc-1.1.2.jar -DgroupId=cassandra -DartifactId=jdbc -Dversion=1.1.2 -Dpackaging=jar
