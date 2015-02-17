metamorphosis
=============

# What is metamorphosis? 

This project is responsible for ALL data going in and out of an external data processing Service (Storm, Hadoop, Spark)

# How is it structured?

The project is a glorified mapper. The two main classes are `SchlossService` and `WorkerService`. Schloss is the centralized distribution center. When a source from s3 needs to be pumped in, it looks at the manifest, and round robins each shard to the worker machines. The workers in turn will process these messages one by one.

# Where do these services live?

The SchlossService will run on the kafka_zookeeper node for the environment. The WorkerServices are currently run on the kafka_brokers themselves. Expect this to change

# What are the messages that External Data Sources (Storm, Hadoop, Spark, etc.) send to metamorphosis?

There are two classes of messages: Source and Sink. Sources need to be loaded from a source type (s3/kinesis), into a kafka topic. Sinks need to do the reverse, take messages from the kafka topics. 

# What are the complications that I need to be aware of?

## Topic fairness
  
Each message schloss expects that a worker will be assigned to this task as soon as possible, without having to wait for existing jobs to finish. This is accomplished by having two levels of round-robins. For example, looking at sources: Each shard of a manifest is handed over to a set of workers uniformly. Each of these workers then place the message in a `RoundRobinByTopicMessageQueue`, allowing each topic to guarantee that it'll see a worker before completing a single topic.

## Streaming

When sinking from a buffer that Storm/Hadoop/Spark is writing to, we have to keep looking at the topic for updated messages. In between these checks we round-robin to see of other topics have new messages. Same for kinesis reads.

