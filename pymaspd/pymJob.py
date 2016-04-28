import logging


LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)
LOG.addHandler(logging.StreamHandler())

class pymJob(object):
    """
    pynJob is the abstraction of a Pynocchio Job
    """
    __doc__ = "Abstract Definition of a Job, does nothing."

    def description(self):
        raise NotImplementedError

    def get_subjobs(self):
        _, subjoblist = self.description()
        return subjoblist

    @classmethod
    def create_job(cls, jobid):
        LOG.debug("Factory is going to create class %s" % cls.__name__)
        return cls(jobid)

    def __init__(self, jobid):
        self.jobid = jobid
        self.journal_id = None
        self.running = False

    def ismutable(self):
        return False

    def isrunning(self):
        return self.running

    def appendjob(self, job):
        if self.ismutable():
            raise NotImplementedError
        return False

    def deletejob(self, job):
        if self.ismutable():
            raise NotImplementedError
        return False

    def insertjobafter(self, newjob, refjob):
        if self.ismutable():
            raise NotImplementedError
        return False

    def movejob(self, job, n):
        if self.ismutable():
            raise NotImplementedError
        return False

    def updatejob(self, *args):
        return False

    def getsettings(self):
        return {}

    async def run(self, parentId):
        # nop
        return True

class pynJobException(Exception):
    pass


class pynJobFactory:
    @staticmethod
    def createJob(classid, jobid):
        LOG.debug("Checking for class %s" % classid)
        for jobs in pymJob.__subclasses__(): # potentially harmful if one can manipulate subclasses to load malicious jobs.
            if classid == jobs.__name__:
                LOG.debug("Success for class %s" % jobs.__name__)
                return jobs.create_job(jobid)
        return None

