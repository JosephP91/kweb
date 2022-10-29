import asyncio

from tornado.options import define, options
from tornado.web import Application

from modules.config import ConfigReader
from modules.consumer import ConsumerWebSocketHandler
from modules.logger import LoggerFactory

config = ConfigReader.read("local")
logger = LoggerFactory.get_logger(config)

define("port", default=config.server.port, help="Run on the given port", type=int)


parameters = dict(config=config, logger=logger)

handlers = [
	("/kweb/consumer/ws/v1", ConsumerWebSocketHandler, parameters)
]


async def main():
	application = Application(
		handlers,
		websocket_ping_interval=config.server.websocket_ping_interval_sec,
		websocket_ping_timeout=config.server.websocket_ping_timeout_sec,
		autoreload=config.server.autoreload
	)

	application.listen(options.port)
	logger.info("Application listening on port {}".format(options.port))
	shutdown_event = asyncio.Event()
	await shutdown_event.wait()


if __name__ == "__main__":
	try:
		logger.info("Starting application ...")
		asyncio.run(main())
	except KeyboardInterrupt:
		logger.info("Application stopped.")
