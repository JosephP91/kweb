import asyncio

from tornado.ioloop import IOLoop
from tornado.web import Application
from tornado.options import define, options

from modules.config import ConfigReader
from modules.logger import LoggerFactory, LoggerType
from modules.consumer import ConsumerWebSocketHandler
from modules.command import CommandParserFactory


# Read configuration file
config_reader = ConfigReader()
config = config_reader.read("local")

# Create the application logger
logger = LoggerFactory.get_logger(config, LoggerType.SCREEN)

# Instantiate the requested command parser
parser = CommandParserFactory.get_parser(config)

# Define tornado options
define("port", default=config.server.port, help="Run on the given port", type=int)

# Define application request handlers.
handlers = [
    ("/kweb/consumer/ws/v1", ConsumerWebSocketHandler, dict(config=config, logger=logger, parser=parser))
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

