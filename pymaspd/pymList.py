import logging
import asyncio
import json
import pymJournal
from pymJob import *

class pymList(pymJob):
    """
    pynList is a List of Jobs done sequentially
    """
    __doc__ = "Work a list of jobs sequentially"

    def __init__(self, jobid):
        super().__init__(jobid)
        self.running = False
        self.joblist = []
        self.paused = False

    def description(self):
        return "Do:", self.joblist # this works really shitty for readable titles in our joblist.

    def ismutable(self):
        return True

    def isrunning(self):
        return self.running

    def appendjob(self, job):
        if isinstance(job, pymJob):
            self.joblist.append(newjob)
            return True
        else:
            logging.warning("Supplied Job not valid")
            return False


    def deletejob(self, job):
        if job in self.joblist:
            if not job.isrunning:
                logging.warning("Can't remove running Job")
                return False
            else:
                try:
                    self.joblist.remove(job)
                except ValueError:
                    logging.warning("Couldn't remove Job")
                    return False
                else:
                    return True
        logging.warning("Couldn't find Job")
        return False



    def insertjobafter(self, newjob, refjob):
        """
        Add a pynJob after a referenced pynJob
        :param newjob: Job to be added
        :param ref: Reference to pynJob after which newjob is to be inserted
        :return: boolean success.
        """
        if refjob is None or not isinstance(refjob, pymJob):
            logging.warning("Referenced Job does not exist")
            return False
        for job in self.joblist:
            if job is refjob:
                self.joblist.insert(self.joblist.index(job)+1, newjob)
                return True
            elif job.ismutable:
                if job.insertjobafter(newjob, refjob):
                    return True
        logging.warning("Referenced Job not found")
        return False

    def movejob(self, job, n):
        """
        Move job n elements on the list
        :param ref: Reference to pynJob
        :return:
        """
        if job.isrunning:
            logging.warning("CanÄt move running job")
            return False
        for j in self.joblist:
            if j is job:
                index = self.joblist.index(j)
                if (index+n)<0 or (index+n)>len(self.joblist):
                    logging.warning("Supplied index out of bounds")
                    return False
                if self.joblist[index+n].istrunning():
                    logging.warning("Can't mutate with running job")
                    return False
                # try to remove job from list
                res = self.joblist.remove(j)
                if not res:
                    logging.warning("Couldn't move job")
                    return False
                # now insert the job at the new position
                res = self.joblist.insert(index+n, j)
                if res:
                    return True
                else:
                    logging.warning("Couldn't reinsert job")
                    return False
            # check if job could be in sublist
            elif j.ismutable():
                if j.movejob(job, n):
                    return True
        # loop finished without finding the reference
        logging.warning("Referenced Job not found")
        return False

    def updatejob(self, settings_dict):
        return True

    def getsettings(self):
        return {}

    async def run(self, parent_id):
        #TODO Acquire JournalId
        journal_id = pymJournal.add_job(parent_id, title=str(self.description()))
        self.running = True
        res = []
        try:
            while True:
                if self.paused:
                    # currently no jobs, we can wait
                    await asyncio.sleep(1)
                if len(self.joblist)==0:
                    # all jobs done
                    break
                job = self.joblist[0]
                result = await job.run(journal_id)
                if callable(result):
                    res.append(await result())
                else:
                    res.append(result)
                self.joblist.remove(0)
            pymJournal.update_job(journal_id, title=str(self.description()), json_meta=json.dump({'assoc_list':res}))
            self.running = False
            return journal_id
        except asyncio.CancelledError:
            # implement shutdown if we cancel this
            #TODO
            pass