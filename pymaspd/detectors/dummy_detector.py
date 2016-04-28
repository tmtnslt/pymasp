from pymDetector import *
import asyncio
import numpy as np
import pymJournal


class pynDummyDetector(pymDetector):
    """ Implementation of a DummyDetector, which will collect ramp data. This is mainly for testing purposes.
        To see a model implementation of a real world detector check TODO
    """
    def description(self):
        return ("Dummy Detector", None)

    def __init__(self, jobid):
        super().__init__(jobid)
        self.journal_id = None
        self.gain = 1.0
        self.running = False

    def _initialize(self):
        return True

    def unload(self):
        pass

    def getsettings(self):
        return {
            'gain': {'current': self.gain, 'type': 'double', 'hint': 'Parameter Gain', 'ro': False},
        }

    def updatejob(self, settings_dict):
        res = False
        if 'gain' in settings_dict:
            self.gain = settings_dict["gain"]
            res = True

        return res


    def pre_run(self):
        pass

    def pre_acquire(self):
        pass

    def acquire(self):
        pass

    def post_acquire(self):
        pass

    def post_run(self):
        pass

    async def run(self, parent_id):
        # Acquire a journal id
        self.journal_id = pymJournal.add_job(parent_id, title=str(self.description()))
        self.running = True
        # Return function call for late collection
        return self.late_collection


    async def late_collection(self):
        jid = self.journal_id
        # generate database entry with ramp values
        data_id =  pymJournal.add_data(np.arange(0, 255 * self.gain, self.gain), self.journal_id)
        # update job entry to chain to our added data
        pymJournal.update_job(jid, assoc_data=data_id)
        # clean up instance variables for next call
        self.journal_id = None
        self.running = False
        return jid