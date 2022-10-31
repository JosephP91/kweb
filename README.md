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

* ### ***Subscribe***

* ### ***Unsubscribe***

* ### ***Topics***

* ### ***Subscriptions***

* ### ***Seek to end***

* ### ***Seek to beginning***

* ### ***Assign***

* ### ***Assignment***

* ### ***Commit async***

* ### ***Commit***
