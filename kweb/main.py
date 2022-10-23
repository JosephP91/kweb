import asyncio

from tornado.ioloop import IOLoop
from tornado.web import Application
from tornado.options import define, options

from modules.config import ConfigReader
from modules.logger import LoggerFactory, LoggerType
from modules.server import WebSocketController


# Read configuration file
config_reader = ConfigReader()
config = config_reader.read("local")

# Create the application logger
logger = LoggerFactory.get_logger(config, LoggerType.SCREEN)

# Define tornado options
define("port", default=config.server.port, help="Run on the given port", type=int)



async def main():
    application = Application(
            [("/websocket", WebSocketController, dict(config=config, logger=logger))],
            websocket_ping_interval=10,
            websocket_ping_timeout=30,
            autoreload=True
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

