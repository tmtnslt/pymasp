import pickle
import json
import zmq
import zmq.asyncio

import asyncio
import logging

from pymaspd_worker import pymaspd_worker
from pymParameter import *

import pymException

# configure which detectors you want to use
# configure which parameters (stages etc.) you want to use
# configure which other jobs (kinds of loops) you want to use
# (probably safe to leave them all enabled)

logging.basicConfig(level=logging.DEBUG)
PYTHONASYNCIODEBUG=1

class pymaspd:
    """
    Pymaspd Master controlls the Job in the Worker, communicates with the outside world with ZeroMQ, has an eye on the Journal
    and everything else you want
    """
    def __init__(self):
        self.worker = pymaspd_worker()
        self.context =  zmq.asyncio.Context()
        #self.context = zmq.Context()
        self.shutdown = False
        self.version = "PynocchioMaster 0.1"
        self.job_counter = 0

        # set listening addr. Currently the frontend will run on the same mashine, so we can use inproc communication
        self.rpc_addr = "tcp://127.0.0.1:80555"

    def list_available_jobs(self):
        alljobs = pymJob.__subclasses__()
        res = []
        for job in alljobs:
            res.append({'classname': job.__name__, 'description': job.__doc__})
            # also check subclasses (pynDetector's!)
            res.extend({'classname': sjob.__name__, 'description': sjob.__doc__} for sjob in job.__subclasses__())
        return res

    def list_available_parameters(self):
        alljobs = pymParameter.__subclasses__()
        res = []
        for job in alljobs:
            res.append({'classname': job.__name__, 'description': job.__doc__})
        return res


    def createjob(self,job):
        logging.debug("Use the Factory to create Job")
        try:
            newjob = pymJobFactory.createJob(job, self.job_counter)
        finally:
            # always increase job_counter to give unique ids.
            self.job_counter += 1
        return newjob

    def unpicklejob(self,pickeld_job):
        """
        Unpickles a job to load saved experiments. Warning: This is currently unrestricted and allows for
        remote code execution!
        :param pickeld_job:
        :return:
        """
        try:
            newjob = pickle.loads(pickeld_job)
        except pickle.UnpicklingError:
            logging.warning("Couldn't load job from pickle")
            raise pymException.pymJobNotExist
        if isinstance(newjob, pymJob):
            return newjob
        else:
            raise pymException.pymJobNotExist



    def main_loop(self):
        # set the context for zmq


        # we want to be able to use the zmq polls
        rpcloop = zmq.asyncio.ZMQEventLoop()
        asyncio.set_event_loop(rpcloop)

        # setup tasks
        tasks=[
            self.worker_loop(),
            self.rpc_loop()
        ]
        # now start the zmq message loop
        rpcloop.run_until_complete(asyncio.wait(tasks))


    async def rpc_loop(self):
        #initialize connection
        sock = self.context.socket(zmq.REP)
        logging.debug("Trying to bind to socket")
        sock.bind(self.rpc_addr)
        logging.debug("Socket Opened, awaiting message")
        while not self.shutdown:
            # wait for messages
            try:
                logging.debug("Wait for message")
                msg = await sock.recv()
                # Currently we are using json to communicate on the protocol level and then we maybe will
                # have a pickle as payload.
                # For some reason we can't await sock.recv_json(). Therefore: Call recv and decode JSON by hand
                # Consider moving this logic to process_rpc...
                if isinstance(msg, bytes):
                    msg = msg.decode('utf8')
                msg = json.loads(msg)
            except Exception as exc:
                # Dangerous! We should rather do exception handling for seperate cases: zmq errors and json errors.
                logging.debug("Error in receiving Message: %s" % exc)
                raise
            logging.debug("Received Message from RPC, going to process")
            # process directly
            reply = self.process_rpc(msg)
            logging.debug("Processing of RPC Message done, going to send reply")

            if reply:
                # response can be slow
                await sock.send_json(reply)
                logging.debug("RPC Message send, returning to start of loop")

        #socket was closed, complete this task


    def process_rpc(self, msg):
        """
        Process Messages from RPC. msg is built as a dict containing the command as the key and the payload as value.
        :param msg:
        :return:
        """
        logging.debug(msg)
        reply = {}

        """
        Worker related commands
        """

        # pause or continue worker loop
        if "start" in msg:
            self.worker.paused = False

        if "pause" in msg:
            self.worker.paused = True

        # list jobs in queue
        if "get_job_queue" in msg:
            reply["job_queue"] = self.worker.unrolllist()

        # get available jobs
        if "list_jobs" in msg:
            reply["job_list"] = self.list_available_jobs()

        if "list_parameters" in msg:
            reply["parameter_list"] = self.list_available_parameters()

        # manipulate queue
        if "move_job" in msg:
            try:
                # obtain reference to instance
                logging.debug("Trying to move Job")
                reply["move_job"] = self.worker.movejob(self.worker.id2job(msg["move_job"][0]),msg["move_job"][1])
                logging.debug("Moved Job")
            except pymException.pymJobRunning:
                reply["error"] = "Can't move running job or swap with one"
                reply["move_job"] = False
            except pymException.pymJobNonMutable:
                reply["error"] = "Can't move job, it is immutable."
                reply["move_job"] = False
            except pymException.pymJobNotFound:
                reply["error"] = "Referenced job not found."
                reply["move_job"] = False
            except pymException.pymJobNotExist:
                reply["error"] = "Job doesn't exist."
                reply["move"] = False
            except AttributeError:
                reply["error"] = "Supplied parameter out of bounds"
                reply["move"] = False

        # add new job to end of some list
        if "add_job_new" in msg:
            try:
                tref = None
                logging.debug("Creating New Job")
                if len(msg["add_job_new"])==2:
                    # reference supplied, add it there
                    try:
                        # obtain referenced job instance
                        tref = self.worker.id2job(msg["add_job_new"][1])
                    except pymException.pymJobNotFound:
                        reply["error"] = "Couldn't find reference to add Job to."
                        reply["add_job_new"] = False
                if "add_job_new" not in reply:
                    tjob = self.createjob(msg["add_job_new"][0])
                    if tjob is not None:
                        logging.debug("Succesfully Created New Job")
                        # now try to attatch the job somewhere
                        if self.worker.appendjob(tjob,tref):
                            logging.debug("Succesfully Added Job To List")
                            reply["add_job_new"] = True
            except pymException.pymJobNonMutable:
                reply["error"] = "Can't add job to referenced job: reference is inmutable."
                reply["add_job_new"] = False
            except pymException.pymJobNotFound:
                reply["error"] = "Referenced job not found."
                reply["add_job_new"] = False
            except pymException.pymJobNotExist:
                reply["error"] = "Job can't be created: Job doesn't exist."
                reply["add_job_new"] = False

        # add new job after a referenced job
        if "add_job_new_after" in msg:
            try:
                logging.debug("Creating New Job")
                if len(msg["add_job_new_after"])==2:
                    try:
                        # obtain referenced job instance
                        tref = self.worker.id2job(msg["add_job_new_after"][1])
                    except pymException.pymJobNotFound:
                        reply["error"] = "Couldn't find reference to add Job to."
                        reply["add_job_new_after"] = False
                    else:
                        # only create job if we found that reference
                        tjob = self.createjob(msg["add_job_new_after"][0])
                        logging.debug("Succesfully Created New Job")
                        if (self.worker.insertjobafterref(tjob,tref)):
                            logging.debug("Succesfully Added Job after Reference")
                            reply["add_job_new_after"] = True

                else:
                    # someone made a mistake
                    reply["error"] = "Protocol Error"
                    reply["add_job_new_after"] = False
            except pymException.pymJobNonMutable:
                reply["error"] = "Can't add job to referenced job: reference is inmutable."
                reply["add_job_new_after"] = False
            except pymException.pymJobNotFound:
                reply["error"] = "Referenced job not found."
                reply["add_job_new_after"] = False
            except pymException.pymJobNotExist:
                reply["error"] = "Job can't be created: Job doesn't exist."
                reply["add_job_new_after"] = False

        # add a pickled job at the end of a list
        if "add_job_pickle" in msg:
            try:
                tref = None
                logging.debug("Unpickle Job")
                if len(msg["add_job_pickle"])==2:
                    # reference supplied, add it there
                    try:
                        # obtain referenced job instance
                        tref = self.worker.id2job(msg["add_job_pickle"][1])
                    except pymException.pymJobNotFound:
                        reply["error"] = "Couldn't find reference to add Job to."
                        reply["add_job_pickle"] = False
                        tjob = None
                if not "add_job_pickle" in reply:
                    # no errors so far
                    tjob = self.unpicklejob(msg["add_job_pickle"][0])
                    if tjob is not None:
                        logging.debug("Succesfully Unpickled Job")
                        # now try to attatch the job somewhere
                        if self.worker.appendjob(tjob,tref):
                            logging.debug("Succesfully Added Job To List")
                            reply["add_job_pickle"] = True
            except pymException.pymJobNonMutable:
                reply["error"] = "Can't add job to referenced job: reference is inmutable."
                reply["add_job_pickle"] = False
            except pymException.pymJobNotFound:
                reply["error"] = "Referenced job not found."
                reply["add_job_pickle"] = False
            except pymException.pymJobNotExist:
                reply["error"] = "Job can't be created: Job doesn't exist."
                reply["add_job_pickle"] = False

        # add a pickled job after a referenced job
        if "add_job_pickle_after" in msg:
            try:
                logging.debug("Unpickle Job")
                if len(msg["add_job_pickle_after"])==2:
                    try:
                        # obtain referenced job instance
                        tref = self.worker.id2job(msg["add_job_pickle_after"][1])
                    except pymException.pymJobNotFound:
                        reply["error"] = "Couldn't find reference to add Job to."
                        reply["add_job_pickle_after"] = False
                    else:
                        # only create job if we found that reference
                        tjob = self.unpicklejob(msg["add_job_pickle_after"][0])
                        logging.debug("Succesfully Unpickled Job")
                        if (self.worker.insertjobafterref(tjob,tref)):
                            logging.debug("Succesfully Added Job after Reference")
                            reply["add_job_pickle_after"] = True

                else:
                    # someone made a mistake
                    reply["error"] = "Protocol Error"
                    reply["add_job_pickle_after"] = False
            except pymException.pymJobNonMutable:
                reply["error"] = "Can't add job to referenced job: reference is inmutable."
                reply["add_job_pickle_after"] = False
            except pymException.pymJobNotFound:
                reply["error"] = "Referenced job not found."
                reply["add_job_pickle_after"] = False
            except pymException.pymJobNotExist:
                reply["error"] = "Job can't be created: Job doesn't exist."
                reply["add_job_pickle_after"] = False



        # delete job
        if "delete_job" in msg:
            try:
                logging.debug("Getting Reference to Job")
                tjob = self.worker.id2job(msg["delete_job"])
                reply["delete_job"] = self.worker.deletejob(tjob)
            except (pymException.pymJobNotFound, pymException.pymJobNotExist):
                reply["error"] = "Can't delete job: Referenced Job not Found!"
                reply["delete_job"] = False
            except pymException.pymJobRunning:
                reply["error"] = "Can't delete job: Job is currently running!"
                reply["delete_job"] = False
            except pymException.pymJobNonMutable:
                reply["error"] = "Can't delete job: Parent Job marks it as inmutable."
                reply["delete_job"] = False


        if "get_settings" in msg:
            try:
                tref = self.worker.id2job(msg["get_settings"])
                reply["get_settings"] = tref.getsettings()
            except (pymException.pymJobNotFound, pymException.pymJobNotExist):
                reply["error"] = "Can't get settings: Referenced Job not Found!"
                reply["get_settings"] = False
            except pymException.pymJobRunning:
                reply["error"] = "Can't get settings: Job is currently running!"
                reply["get_settings"] = False
            except NotImplementedError:
                reply["error"] = "Can't get settings: Job has no settings!"
                reply["get_settings"] = False

        if "update_settings" in msg:
            try:
                tref = self.worker.id2job(msg["update_settings"][0])
                reply["update_settings"] = tref.updatejob(msg["update_settings"][1])
            except (pymException.pymJobNotFound, pymException.pymJobNotExist):
                reply["error"] = "Can't set settings: Referenced Job not Found!"
                reply["get_settings"] = False
            except pymException.pymJobRunning:
                reply["error"] = "Can't set settings: Job is currently running!"
                reply["get_settings"] = False
            except pymException.pymOutOfBound:
                reply["error"] = "Can't set settings: Parameter out of bound!"
                reply["get_settings"] = False
            except NotImplementedError:
                reply["error"] = "Can't set settings: Job has no settings!"
                reply["get_settings"] = False
            except (AttributeError, TypeError):
                reply["error"] = "Can't set settings: Faulty settings supplied!"
                reply["get_settings"] = False
            except IndexError:
                reply["error"] = "Can't set settings: Faulty settings supplied!"
                reply["get_settings"] = False

        # pickle existing job
        if "save_job" in msg:
            try:
                logging.debug("Saving Job as a pickle")
                tjob = self.worker.id2job(msg["save_job"])
            except (pymException.pymJobNotFound, pymException.pymJobNotExist):
                reply["error"] = "Can't save job: Referenced Job not Found!"
            else:
                try:
                    reply["save_job"] = pickle.dumps(tjob)
                except pickle.PickleError as err:
                    reply["error"] = "Job can't be pickled. Ask a developer."
                    logging.warning("Couldn't pickle job %s: %s" % (tjob, err))
                    reply["save_job"] = False

        """
        Journal related commands
        """

        if "create_new_database" in msg:
            """
            Creates new database file and uses it, requires main loop to be stopped
            """
            pass

        if "load_database" in msg:
            """
            Loads an existing database file and uses it, requires main loop to be stopped
            """
            pass

        if "archive_database" in msg:
            """
            Transform existing database to long term storage file, requires main loop to be stopped
            """
            pass

        if "delete_database" in msg:
            """
            Delete database file on disk. Requires main loop to be stopped and a flag to be set in the pymaspd settings
            """
            pass

        if "list_experiments" in msg:
            """
            Return a list of experiments from the current database
            """
            pass

        if "list_jobs" in msg:
            """
            Return a list of all jobs from the current database. Expert option, requires a flag to be set in the pymaspd settings
            """
            pass

        if "get_1d_data" in msg:
            """
            Try to obtain a 1d representation of data associated with supplied job/experiment
            """
            pass

        if "get_2d_data" in msg:
            """
            Try to obtain a 2d representation of data associated with supplied job/experiment
            """
            pass

        if "get_full_data" in msg:
            """
            Try to obtain all data associated with supplied job/experiment and return it as raw as possible
            """
            pass



        #TODO: Labbook functions...

        """
        Daemon related commands
        """

        #TODO

        # respond to a ping
        if "ping" in msg:
            reply["pong"]= self.version

        # respond to special ping
        if "workerping" in msg:
            reply["workerpong"] = self.worker.get_version()

        if "shutdown" in msg:
            logging.debug("Received Shutdown Signal")
            self.shutdown = True
            reply["shutdown"] = "Goodbye"
        #TODO we should evaluate errors up until here and then include them in the error field


        return reply

    async def worker_loop(self):
        while not self.shutdown:
            await self.worker.job_loop()


if __name__ == "__main__":
    proc = pymaspd()
    print(isinstance(proc.worker_loop, asyncio.futures.Future))
    print(asyncio.iscoroutine(proc.worker_loop))
    proc.main_loop()