class FailedToBind(Exception):
    """Failed to Bind the ORM to the Database."""

    pass


class MultipleObjectsReturned(Exception):
    """The query returned multiple objects when only one was expected."""

    pass


class ObjectDoesNotExist(Exception):
    """The requested object does not exist"""

    silent_variable_failure = True


class OperationalError(Exception):
    """The database Engine returned an error."""

    pass
