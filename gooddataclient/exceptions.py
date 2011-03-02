
class AuthenticationError(Exception):
    pass

class ProjectNotOpenedError(Exception):
    pass

class ProjectNotFoundError(Exception):
    pass

class DataSetNotFoundError(Exception):
    pass

class UploadFailed(Exception):
    pass

class MaqlExecutionFailed(Exception):
    pass
