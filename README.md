# Kafka Streams Consumer Code Examples

[Python Consumer](/consumer.py)

[Go Consumer](/consumer.go)

[NodeJS Consumer](/consumer.js)

[Java Example](/Java/)

## Pre-requisites

The following constants you have to receive from our suport team before running examples:

```
	const username = "<YOUR USERNAME>"
	const password = "<YOUR PASSWORD>"
	const topic = "<TOPIC>" // e.g. "tron.broadcasted.transactions"
```

## Certificates

[client.key.pem](/client.key.pem)

[client.cer.pem](/client.cer.pem)

[server.cer.pem](/server.cer.pem)

## Notes

Configuration includes important settings, overriding defaults:

1. Commit turned off to prevent lagging of consumer on re-start. However, this leads to loosing messages between re-starts:
```
"enable.auto.commit":                    false,
```

2. Offset by default set to latest message:

```
"auto.offset.reset":                     "latest",
```

3. Preventing fail to connect due to self-sign certificates:

```
"ssl.endpoint.identification.algorithm": "none",
```

4. Group ID must start with your username:

```
"group.id":                              username + "-mygroup",
```

## Latency measurements

Latency of messages can be caused by several reasons:

1. Consumer can not comsume messages in the rate they coming into topic. This can be caused by insufficient network bandwidth, CPU or other issues on consumer side
2. The lag in your group is accumulated and you need to catch up to the latest offset. To prevent this, we use setting ```"enable.auto.commit": false``` on startup

General idea that if your consumer works correctly, it waits on the line:

```
consumer.Poll(100)
```

for the next message to come. If the message is already in the queue to be polled, it means your consumer is lagging.

Before doing measuring of the topics latency, take the following aspects in the consideration:

1. Examples log messages to console, that may cause persistent lagging, especially for fast topics as broadcasted transactions
2. Examples are single-threaded, in production mode you  need to use multi-threading to consume messages
3. More effective languages are better to use, as rust/java/golang compared to javascript/python
4. Timestamp of transaction included in broadcasted messages are when transaction was created, not broadcasted. It may be several seconds before it was sent by wallet to the node

## Running NodeJS example

```
npm install
npm run start
```

## Using Go Example

1. Initialize your Go module:

```bash
go mod init kafka-consumer
```

This creates a `go.mod` file for your project, enabling dependency management.

2. Install the **confluent-kafka-go** library:

```bash
go get github.com/confluentinc/confluent-kafka-go/kafka
```

### Running the Go Kafka Consumer

After setting up the code, follow these steps to run the Go Kafka consumer:

1. **Initialize the Go module** (if not already done):

```bash
go mod init kafka-consumer
```

2. **Run the Go file**:

```bash
go run consumer.go
```

## Running Java example

- Create a new Java Project and replace contents of default `Main.java` with the contents of the Main file added in the folder.
- Download all the certificates added in the folder and store them in `src/main/resources/` folder.
- Run the Java program to get the data in terminal.


