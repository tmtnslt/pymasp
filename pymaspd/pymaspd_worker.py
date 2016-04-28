import asyncio

import pymJournal
from pymJob import LOG, pymJob


class pymaspd_worker:
    """
    PynocchioWorker is the worker class which holds and runs Pynocchio's Jobs.
    """
    def __init__(self):
        self.version = "pymaspd Worker 0.1"
        self.joblist = []
        self.global_devices_instances = []
        self.job_lut = []
        self.shutdown = False

        #start paused
        self.paused = True

    def get_version(self):
        return self.version

    def unrolllist(self):
        """
        Unroll the JobList to display and edit it.
        :return: [JobDictionary]
        """
        LOG.debug("Unrolling list")
        return [self.genjobdictionary(job) for job in self.joblist]

    def genjobdictionary(self, job):
        """
        Generate a JobDictionary for the Job
        Entries:
            description: Human readable description of the Job
            child: (List of) attached Jobs' JobDictionary
            mutable: Is the Job mutable? (reserved for future uses)
            running: Indicates the Job is already on the TaskList
            ref: weakreference to object
        :param pynJob:
        :return:
        """
        LOG.debug("Generating Entry for Job %s", job)
        if job is not None and isinstance(job, pymJob):
            try:
                # ask the Job element on the list to give us its description an child elements
                description, subjob = job.description()
                LOG.debug("Got description and subjob list")
            except AttributeError:
                # couldn't call description(), maybe it's not a Job? Return an empty element.
                return {"type": None, "description": None, "child": None, "mutable": False, "running": False}

            answ_dict = {"type": type(job).__name__, "description": description, "child": [self.genjobdictionary(sj) for sj in subjob], "mutable": job.ismutable(), "running": job.isrunning(), "id": job.jobid}
            LOG.debug("Reply dict: {}".format(answ_dict))

            return answ_dict
        else:
            LOG.debug("Element is not a valid Job or list is empty")
            return {}

    def id2job(self, id):
        """
        Traverses the job list to find the job with supplied id.
        Future versions of this functions may use a LUT to find id before traversing the list
        """
        for job in self.joblist:
            if job.jobid is id:
                return job
            else:
                tjob = self._traverse_id2job(job, id)
                if tjob is not None:
                    return tjob
        return None

    def _traverse_id2job(self,job_ref, id):
        """
        Subfunction of id2job to recursivly traverse subjobs
        """
        for job in job_ref.get_subjoblist:
            if job.jobid is id:
                return job
            else:
                tjob = self._traverse_id2job(job, id)
                if tjob is not None:
                    return tjob
        return None

    """
    Modify the job list
    """

    def appendjob(self, newjob, refjob=None):
        """
        Add a pynJob to the end of the main list or referenced sublist
        :param newjob: Job to be added
        :param refjob: Instance of pynJob with sublist (optional)
        :return: boolean success.
        """
        if refjob is None:
            self.joblist.append(newjob)
            return True
        else:
            # we need to insert the job into a sublist
            # we should check that referenced object is actually a job...
            if refjob is not None and isinstance(refjob, pymJob):
                if refjob.ismutable:
                    # since we directly call refjob, we don't need to walk any lists, but can
                    # tell it directly to insert newjob
                    return refjob.appendJob(newjob)
                else:
                    LOG.warning("Can't insert Job into non mutable")
                    return False
            else:
                LOG.warning("Referenced Job does not exist")
                return False

    def deletejob(self, refjob):
        """
        Delete a Job from the main list or any sublist
        :param refjob: Instance of  pynJob to be deleted
        :return: boolean success
        """



        #sanitize
        if refjob is None or not isinstance(refjob, pymJob):
            LOG.warning("Referenced Job does not exist")
            return False
        if refjob.isrunning():
            LOG.warning("Can't delete running job")
            return False
        for job in self.joblist:
            if job is refjob:
                # Found job in main list, delete it from here
                self.joblist.remove(job)
                return True
            if job.ismutable:
                # refjob might be in sublist and deletable, call job's own procedure to find it
                if job.deleteJob(refjob):
                    return True
        # None of the above, issue a warning
        LOG.warning("Referenced Job not found")
        return False

    def insertjobafterref(self, newjob, refjob):
        """
        Add a pynJob after a referenced pynJob
        :param newjob: Job to be added
        :param refjob: Instance of pynJob after which newjob is to be inserted
        :return: boolean success.
        """

        if refjob is None or not isinstance(refjob, pymJob):
            LOG.warning("Referenced Job does not exist")
            return False
        for job in self.joblist:
            if job is refjob:
                # Found refjob in main list, add newjob after
                self.joblist.insert(self.joblist.index(job)+1, newjob)
                return True
            elif job.ismutable:
                # refjob might be in a mutable sublist, call job's own procedure to find it
                if job.insertjobafter(newjob, refjob):
                    return True
        # None of the above, issue a warning
        LOG.warning("Referenced Job not found")
        return False

    def updatejob(self, refjob, *args):
        """
        Update a pynJob with the supplied arguments, wrapper to call the pynJob's own updatejob method
        :param refjob: Instance of pynJob
        :param args: arguments which are passed on to reference's updatejob method
        :return: boolean success
        """
        if refjob is None or not isinstance(refjob, pymJob):
            LOG.warning("Referenced Job does not exist")
            return False

        return refjob.updatejob(*args)

    def getsettings(self, refjob):
        """
        Return settings of an pynJob instance. Wraps call to pynJob's own getsettings method
        :param refjob: Instance of pynJob
        :return: settings_dict
        """
        if refjob is None or not isinstance(refjob, pymJob):
            LOG.warning("Referenced Job does not exist")
            return False

        return refjob.getsettings()

    def movejob(self, refjob, n):
        """
        Move job n elements on the list
        :param refjob: Instance of pynJob
        :return:
        """
        if refjob is None or not isinstance(refjob, pymJob):
            LOG.warning("Referenced Job does not exist")
            return False
        if refjob.isrunning():
            LOG.warning("Can't move currently running job")
            return False
        for job in self.joblist:
            if job is refjob:
                index = self.joblist.index(job)
                if (index+n)<0 or (index+n)>len(self.joblist):
                    LOG.warning("Supplied index out of bounds")
                    return False
                if self.joblist[index+n].istrunning():
                    LOG.warning("Can't mutate with running job")
                    return False
                # try to remove job from list
                res = self.joblist.remove(job)
                if not res:
                    LOG.warning("Couldn't move job")
                    return False
                # now insert the job at the new position
                res = self.joblist.insert(index+n, job)
                if res:
                    return True
                else:
                    LOG.warning("Couldn't reinsert job")
                    return False
            # check if job could be in sublist
            elif job.ismutable():
                if job.movejob(refjob, n):
                    return True
        # loop finished without finding the reference
        LOG.warning("Referenced Job not found")
        return False

    """
    Main Loop
    """

    async def job_loop(self):
        """
        coroutine to work through the job list
        :return:
        """
        try:
            LOG.debug("Looking for a job...")
            if len(self.joblist)==0 or self.paused:
                LOG.debug("No Job, going to sleep")
                # currently no jobs, we can wait
                await asyncio.sleep(1)
                return

            self.current_job = self.joblist[0]
            # create a new experiment and root job entry for this job
            result = await self.current_job.run(None)
            if callable(result):
                result = await result()
            pymJournal.add_experiment(self.current_job.description()[0], result)

            # remove job from list. Last reference will be self.current_job until the next one starts.
            self.joblist.remove(0)
        except asyncio.CancelledError:
            # implement shutdown if we cancel this
            #TODO
            pass