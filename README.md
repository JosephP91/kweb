kweb
====

kweb is a Python application that exposes Kafka API via websocket.
The application is based on Tornado asynchronous web framework
and kafka-python library.

Donate
======

Help me to improve this project!

<a href="https://www.bitcoinqrcodemaker.com/pay/?type=2&amp;style=bitcoin&amp;color=1&amp;border=4&amp;address=bc1q3qxm5cryfjxl9gysh9m2kg270zdjcxp2j0lh9p" target="_blank"><img src="https://www.bitcoinqrcodemaker.com/donate_button.png" border="0" width="150" height="36" alt="Donate button" title="Donate" /></a>

Usage
====

kweb is very easy to use. You simply create a websocket connection and
you can start issuing command to operate with kafka.

Start the service
--
Assuming you already have the source code, in order to start the service you can do:

```bash
cd kweb
python main.py
```

Now you have the service up and running.

Consumer API
--

In order to createa Kafka consumer you have to connect via websocket protocol to the following url:

```bash
ws://localhost:8000/kweb/consumer/ws/v1
```

* ### ***Create consumer***

In order to create a consumer, issue the following command on the websocket:

```json
{
    "command_name": "create_consumer", 
    "parameters": {
        "bootstrap_servers": ["<kafka-host-1>:9092", "<kafka-host-2:9092"], 
        "group_id": "testing_group", 
        "enable_auto_commit": false
    }
}
```

Done. Consumer created. If you want to know which options you can specify, you can check here: https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html


* ### ***Subscribe***

After you created a consumer, you should subscribe some topics. You can do that by issuing the following command:

```json
{
    "command_name": "subscribe", 
    "parameters": {
        "topics": ["TOPIC1", "OTHER-TOPIC", "ANOTHER-TOPIC"]
    }
}
```
More details on this command here: https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html#kafka.KafkaConsumer.subscribe


* ### ***Unsubscribe***

If you want to unsubscribe the previously subcribed topics, you can issue the following command:

```json
{
    "command_name": "unsubscribe", 
    "parameters": {}
}
```
More details on this command here: https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html#kafka.KafkaConsumer.unsubscribe


* ### ***Seek to end***

If you want to seek to end of topic-partition, you can issue the following command:

```json
{
    "command_name": "seek_to_end", 
    "parameters": {
        "topic-partitions": [
            {
                "topic": "<topic-name>",
                "partition": 0
            }, 
            {
                "topic": "<another-assigned-topic>",
                "partition": 2
            }
        ]
    }
}
```
More details on this command here: https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html#kafka.KafkaConsumer.seek_to_end


* ### ***Seek to beginning***

If you want to seek to beginning of topic-partition, you can issue the following command:

```json
{
    "command_name": "seek_to_beginning", 
    "parameters": {
        "topic-partitions": [
            {
                "topic": "<topic-name>",
                "partition": 0
            },
			{
                "topic": "<another-assigned-topic>",
                "partition": 2
            }
        ]
    }
}
```
More details on this command here: https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html#kafka.KafkaConsumer.seek_to_beginning


* ### ***Assign***

If you want to assign a series of topic-partition to your consumer you can issue the following command:

```json
{
    "command_name": "assign", 
    "parameters": {
        "topic-partitions": [
            {
                "topic": "<topic-name>",
                "partition": 0
            }, 
            {
                "topic": "<another-assigned-topic>",
                "partition": 2
            }
        ]
    }
}
```
More details on this command here: https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html#kafka.KafkaConsumer.assign


* ### ***Commit Async***

If you want to commit your offset in an asynchronous manner, you can issue the following command:

```json
{
    "command_name": "commit_async", 
    "parameters": {
        "topic-offset-metadata": [
            {
                "key": {
                    "topic": "<topic-name>",
                    "partition": 0
                },
                "value": {
                    "offset": 1,
                    "metadata": "your metadata here"
                }
            }
        ]
    }
}
```

Note that ```topic-offset-metadata``` can also be an empty array. In this way all the offset will be committed. Note also
that since this command is asynchronous, a callback will notify you on the same socket when the commit will be
received by the cluster.

More details on this command here: https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html#kafka.KafkaConsumer.commit_async


* ### ***Commit***

If you want to commit your offset you can issue the following command:

```json
{
    "command_name": "commit", 
    "parameters": {
        "topic-offset-metadata": [
            {
                "key": {
                    "topic": "<topic-name>",
                    "partition": 0
                },
                "value": {
                    "offset": 1,
                    "metadata": "your metadata here"
                }
            }
        ]
    }
}
```

Note that ```topic-offset-metadata``` can also be an empty array. In this way all the offset will be committed.

More details on this command here: https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html#kafka.KafkaConsumer.commit

