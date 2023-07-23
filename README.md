## Hadoop MapReduce Mini Exercise

Welcome to Hadoop exercises from lecture

## Folder Structure

The workspace contains two folders by default, where:

- `src`: the folder to maintain sources
- `lib`: the folder to maintain dependencies
- `data`: location in docker container mounted to local host directory

## Dependency Management

The `JAVA PROJECTS` view allows you to manage your dependencies. More details can be found [here](https://github.com/microsoft/vscode-java-dependency#manage-dependencies). <br>
The `MAVEN` version 7 in compatible with Hadoop Docker image version. <br>
The `APACHE HADOOP-CORE` version 1.2.1

## Commands
To run map-reduce job in prompt:

`hadoop jar MapReduce.jar input/trans.txt MapReduce/output/mapsidejoin`

To start docker image:

`docker-compose up`

> Ensure the input existed in HDFS and the output location hasn't been created in advance. <br>
> Jar file is exported by setting Artifacts in IntelliJ. At the moment, the jar is included in project build and contain one main class only. 
