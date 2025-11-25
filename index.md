# BigQuery Kafka-based TAP service

```{abstract}
Breakdown how we will design & deploy a Kafka-based version of the BigQuery TAP service. Summarize architecture and steps needed to setup both the TAP (Java application) service, as well as the bridge (Python FastAPI side) which will interact with BigQuery to run queries. The interaction between the TAP service and the BigQuery bridge will happen through a Kafka event-bus providing better scalability and support for handling cancellation and monitoring of queries
```

## Add content here

See the [Documenteer documentation](https://documenteer.lsst.io/technotes/index.html) for tips on how to write and configure your new technote.
