class pymJobException(Exception):
    pass


class pymJobNonMutable(pymJobException):
    pass


class pymJobNotFound(pymJobException):
    pass

class pymJobNotExist(pymJobException):
    pass