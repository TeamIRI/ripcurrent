# Ripcurrent

## Synopsis

Ripcurrent is an application that makes use of Debezium connectors to monitor changes in a database, and acts on the changes by dynamically generating SortCL scripts to replicate to data targets. 

Paths to an IRI Data Class library and IRI Rules library can be specified to consistently classify data and apply transformations if there is a paired default rule to the data class.

## Running

Running the command `./gradlew run` will run the application.

## Building

Running the command `./gradlew build` will generate zip and tar distributions.

## Logging

Ripcurrent (and its dependency on Debezium) use log4j 1 for logging messages. In the *Ripcurrent* distribution, log4j configuration can be specified by placing a *log4j.properties* file in the *conf* directory.

## Ripcurrent Configuration Options

Ripcurrent takes in configuration options as Java properties. Ripcurrent will look for a *config.properties* file in the *conf* directory to ingest properties from.

Properties can also be specified by setting the environment variable RIPCURRENT_OPTS to define properties such as '-Dkey="value" -Dkey2="value2"'.

## IRI Workbench Integration

Ripcurrent is available as an optional feature for IRI Workbench. The feature provides the latest distribution of Ripcurrent, along with a wizard and editor that assist in generating a Ripcurrent properties file.
