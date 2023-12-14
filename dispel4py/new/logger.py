import logging

import coloredlogs

# import os

format = "%(asctime)s %(name)s[%(process)d] %(levelname)s %(message)s"
logger = logging.getLogger(__name__)
logger.propagate = False
# coloredlogs.install(level = 'debug', fmt = format, logger = logger)
coloredlogs.install(level="info", fmt=format, logger=logger)
# coloredlogs.install(level = 'error', fmt = format, logger = logger)


logging.getLogger("astropy").setLevel(logging.ERROR)
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)

coloredlogs.install(level="error", fmt=format)


# logger.debug("this is a debugging message")
# logger.info("this is an informational message")
# logger.warning("this is a warning message")
# logger.error("this is an error message")


import inspect


def print_stack_trace():
    stack = inspect.stack()
    # pid = os.getpid()
    # print(f"Current process ID: {pid}")
    for frame in stack:
        print(
            f"Function '{frame.function}' called from file '{frame.filename}' at line {frame.lineno}",
        )


def simpleLogger(self, msg):
    try:
        print(f"{self.id} (rank {self.rank}): {msg}")
    except:
        print(f"{self.id}: {msg}")
