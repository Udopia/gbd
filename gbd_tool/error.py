class GbdApiError(Exception):
    def __init__(self, message):
        self.message = message
        super().__init__(self.message)
    pass


class GbdApiFeatureNotFound(GbdApiError):
    pass


class GbdApiParsingFailed(GbdApiError):
    pass


class GbdApiDatabaseError(GbdApiError):
    pass