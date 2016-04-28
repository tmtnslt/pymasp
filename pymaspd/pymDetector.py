from pymJob import *

class pymDetector(pymJob):
    """ Class to acquire data from a specific detector. Please note that all instances of this class will be deleted
        after they ran. Use this as a wrapper of a module or use class variables if you need variables (such as hardware
        handles) to persist throughout a session.
    """
    def description(self):
        raise NotImplementedError

    def __init__(self, jobid):
        super().__init__(jobid)
        self.initialized = False

    def initialize(self):
        if self.initialized:
            return True
        else:
            return self._initialize()
