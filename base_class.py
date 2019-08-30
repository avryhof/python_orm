import logging
import math
import pprint
import sys

from orm.aware_datetime import aware_datetime

logger = logging.getLogger(__name__)
datetime = aware_datetime()


class BaseClass:
    debug = False
    debug_stdout = False

    init_time = None

    def _debug_handler(self, message, **kwargs):
        pretty = kwargs.get("pretty", False)

        if pretty:
            message = pprint.pformat(message)

        log_message = "%s: %s\n" % (datetime.now().isoformat()[0:19], message)

        if self.debug:
            logger.info(log_message)

    def _timer(self):
        if not self.init_time:
            self.init_time = datetime.now()
            self._debug_handler("Class %s initiated." % self.__class__.__name__)
        else:
            self._debug_handler("Class %s completed." % self.__class__.__name__)

            complete_time = datetime.now()
            command_total_seconds = (complete_time - self.init_time).total_seconds()
            command_minutes = math.floor(command_total_seconds / 60)
            command_seconds = command_total_seconds - (command_minutes * 60)

            self._debug_handler(
                "Class %s was active for %i minutes and %i seconds to run."
                % (self.__class__.__name__, command_minutes, command_seconds)
            )

    def __init__(self, **kwargs):
        self.debug = kwargs.get("debug", False)
        self.debug_stdout = kwargs.get("debug_stdout", False)

        if self.debug_stdout:
            logger.setLevel(logging.INFO)
            logger.addHandler(logging.StreamHandler(sys.stdout))

        if self.debug:
            self._timer()
            self._debug_handler("Debugging enabled.")

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._timer()
