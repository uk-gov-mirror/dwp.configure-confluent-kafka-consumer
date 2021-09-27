# configure-confluent-kafka-consumer

This repo contains deployment code for confluent kafka consumers, which are the apacha branded consumers for the kafka application.

## Usage in DataWorks

These consumers are no longer used as from the original investigations it was decided that they were not customisable enough to do the things we needed them to do.

## Replacement

A custom Kotlin consumers has been created instead which performs everything we need it to do called [kafka-to-hbase](https://github.com/dwp/kafka-to-hbase).
