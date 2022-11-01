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

In order to create a Kafka consumer you have to connect via websocket protocol to the following url:

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

Done. Consumer created. If you want to know which options you can specify, you can check here: 
<a href="https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html" target="_blank">Kafka Consumer</a>


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
More details on this command here: 
<a href="https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html#kafka.KafkaConsumer.subscribe" target="_blank">Subscribe Command</a>


* ### ***Unsubscribe***

If you want to unsubscribe the previously subcribed topics, you can issue the following command:

```json
{
    "command_name": "unsubscribe", 
    "parameters": {}
}
```
More details on this command here: 
<a href="https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html#kafka.KafkaConsumer.unsubscribe" target="_blank">Unsubscribe Command</a>


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
More details on this command here: 
<a href="https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html#kafka.KafkaConsumer.seek_to_end" target="_blank">Seek to End Command</a>


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
More details on this command here: 
<a href="https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html#kafka.KafkaConsumer.seek_to_beginning" target="_blank">Seek to Beginning Command</a>


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
More details on this command here: 
<a href="https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html#kafka.KafkaConsumer.assign" target="_blank">Assign Command</a>


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

More details on this command here: 
<a href="https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html#kafka.KafkaConsumer.commit_async" target="_blank">Commit Async Command</a>


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

More details on this command here: 
<a href="https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html#kafka.KafkaConsumer.commit" target="_blank">Commit Command</a>


* ### ***Committed***

If you want to know the last committed offset for a given topic-partition, you can issue the following command:

```json
{
    "command_name": "committed", 
    "parameters": {
        "topic-partition": {
            "topic": "<topic-name>",
            "partition": 0
        },
        "metadata": false
    }
}
```
If you don't want only the last committed offset as an integer, you can set ```metadata``` to ```true```. In this way
more details will be returned.

More details on this command here: 
<a href="https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html#kafka.KafkaConsumer.committed" target="_blank">Committed</a>


* ### ***Partitions for topic***

If you want to know the partitions available for a specific topic (assuming you are authorized to do so),
you can issue the following command:

```json
{
    "command_name": "partitions_for_topic", 
    "parameters": {
        "topic": "<topic-name>"
    }
}
```
More details on this command here: 
<a href="https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html#kafka.KafkaConsumer.partitions_for_topic" target="_blank">Partitions for Topic Command</a>


* ### ***Position***

If you want to know the offset of the next record that will be fetched you can issue the following command:

```json
{
    "command_name": "position", 
    "parameters": {
        "topic-partition": {
            "topic": "<topic-name>",
            "partition": 0
        }
    }
}
```
More details on this command here: 
<a href="https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html#kafka.KafkaConsumer.position" target="_blank">Position Command</a>


* ### ***Highwater***

If you want to know the last known highwater for a given topic-partition, you can issue the following command:

```json
{
    "command_name": "highwater", 
    "parameters": {
        "topic-partition": {
            "topic": "<topic-name>",
            "partition": 0
        }
    }
}
```
If you want to know what's an highwater offset, or you want to have more details on this command, go here:
<a href="https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html#kafka.KafkaConsumer.highwater" target="_blank">Highwater Command</a>


* ### ***Pause***

If you want to suspend fetching from a set of topic-partitions, you can issue the following command:

```json
{
    "command_name": "pause", 
    "parameters": {
        "topic-partition": [
            {
                "topic": "<topic-name>", 
                "partition": 0
            }
        ]
    }
}
```
More details on this command here: 
<a href="https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html#kafka.KafkaConsumer.pause" target="_blank">Pause Command</a>


* ### ***Resume***

If you want to resume fetching from a set of previously paused topic-partitions, you can issue the following command:

```json
{
    "command_name": "resume", 
    "parameters": {
        "topic-partition": [
            {
                "topic": "<topic-name>", 
                "partition": 0
            }
        ]
    }
}
```
More details on this command here: 
<a href="https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html#kafka.KafkaConsumer.resume" target="_blank">Resume Command</a>


* ### ***Seek***

If you want to seek on specifically topic-partion, you can issue the following command:

```json
{
    "command_name": "seek", 
    "parameters": {
        "topic-partition": {
            "topic": "<topic-name>", 
            "partition": 0
        },
        "offset": 100
    }
}
```
More details on this command here: 
<a href="https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html#kafka.KafkaConsumer.seek" target="_blank">Seek Command</a>


* ### ***Beginning Offsets***

If you want to get the initial offsets for the specified set of topic-partitions, you can issue the following command:

```json
{
    "command_name": "beginning_offsets", 
    "parameters": {
        "topic-partition": [
            {
                "topic": "<topic-name>", 
                "partition": 0
            }
        ]
    }
}
```
More details on this command here: 
<a href="https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html#kafka.KafkaConsumer.beginning_offsets" target="_blank">Beginning Offsets Command</a>


* ### ***End Offsets***

If you want to get the end offsets for the specified set of topic-partitions, you can issue the following command:

```json
{
    "command_name": "end_offsets", 
    "parameters": {
        "topic-partition": [
            {
                "topic": "<topic-name>", 
                "partition": 0
            }
        ]
    }
}
```
More details on this command here: 
<a href="https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html#kafka.KafkaConsumer.end_offsets" target="_blank">End Offsets Command</a>
