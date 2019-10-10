import datetime
from time import strftime, gmtime

import pytz as pytz


class aware_datetime:
    timezone = None
    value = None

    def __init__(self, *args, **kwargs):
        self.timezone = kwargs.pop("timezone", strftime("%z", gmtime()))
        if len(args) > 0 or len(list(kwargs.items())) > 0:
            self.value = datetime.datetime(*args, **kwargs)
        else:
            self.value = datetime.datetime.now()

    @property
    def _is_aware(self):

        return self.value.tzinfo is None or self.value.tzinfo.utcoffset(self.value) is None

    def _aware(self, new_value=False):
        """
        Adds Timezone info to a date.
        :param input_date:
        :return:
        """
        if new_value:
            self.value = new_value

        if not self._is_aware:
            try:
                localtz = pytz.timezone(self.timezone)
                self.value = localtz.localize(self.value)
            except AttributeError:
                pass

        return self.value

    def now(self):
        self.value = datetime.datetime.now()

        return self._aware()

    def timedelta(self, *args, **kwargs):

        return datetime.timedelta(*args, **kwargs)

    def __str__(self):

        return str(self._aware())
