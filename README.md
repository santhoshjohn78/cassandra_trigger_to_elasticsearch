# cassandra_trigger_to_elasticsearch
A generic cassandra trigger that pushes the inserted row into ElasticSearch index with the same names as the cassandra column family


## Getting Started

Clone or download this project from github and use your fav IDE to import as a maven project.

```
git clone <url>
mvn clean install -Penv-dev
```

### Prerequisites

This project needs ElasticSearch 5.6 running somewhere. Update the property file with
the host and port. This also needs cassandra 2.x and above

```
elasticsearch.host=localhost
elasticsearch.port=9200
```

### Installing

This is built on maven

```
mvn clean install -Penv-dev
```

copy the jar ./cassandra-triggers-to-elasticsearch-1.0-SNAPSHOT-jar-with-dependencies.jar to cassandra triggers dir

using nodetools load the jar and use cql to create the trigger.

If you get classloader errors restart cassandra.

```
cp ./cassandra-triggers-to-elasticsearch-1.0-SNAPSHOT-jar-with-dependencies.jar $CASSANDRAHOME/conf/triggers

$CASSANDRAHOME/bin/nodetools reloadtriggers

$CASSANDRAHOME/bin/cqlsh

cqlsh>use <keyspace>
cqlsh:keyspace>CREATE TRIGGER <name> ON [<keyspace>.]<table> USING 'ehss.cassandra.triggers.elasticsearch.ElasticSearchTrigger' 

```


## Built With

* [Maven](https://maven.apache.org/) - Dependency Management


## Authors

* **Santhosh John** - *Initial work* - [EHSS](https://github.com/santhoshjohn78)

## License

This project is licensed under the MIT License - see the [LICENSE.md](LICENSE.md) file for details

## Acknowledgments

* Hat tip to anyone whose code was used
* Inspiration
* etc