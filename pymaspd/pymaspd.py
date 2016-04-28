import pickle

import zmq
import zmq.asyncio

from pymaspd_worker import pymaspd_worker
from pymParameter import *

# configure which detectors you want to use
# configure which parameters (stages etc.) you want to use
# configure which other jobs (kinds of loops) you want to use
# (probably safe to leave them all enabled)

LOG = logging.getLogger(__name__)
LOG.setLevel(logging.DEBUG)
LOG.addHandler(logging.StreamHandler())


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


    def createjob(self,job,ref=None):
        LOG.debug("Use the Factory to create Job")
        newjob = pynJobFactory.createJob(job, self.job_counter)
        # increase job_counter to give unique ids.
        self.job_counter += 1
        LOG.debug("Append the Job to our Joblist")
        # for now appendjob will return True or False on success. Consider returning a weakref in future versions.
        return self.worker.appendjob(newjob, ref)

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
        LOG.debug("Trying to bind to socket")
        sock.bind(self.rpc_addr)
        LOG.debug("Socket Opened, awaiting message")
        while not self.shutdown:
            # we are only communicating with a python frontend, so pyobj will be used
            # if you want to change this, you have to take care of serialization!

            # wait for messages
            LOG.debug("Wait for message")
            msg = await sock.recv()
            LOG.debug("Received Message from RPC, going to process")

            # process directly
            reply = self.process_rpc(pickle.loads(msg))
            LOG.debug("Processing of RPC Message done, going to send reply")

            if reply:
                # response can be slow
                await sock.send_pyobj(reply)
                LOG.debug("RPC Message send, returning to start of loop")

        #socket was closed, complete this task


    def process_rpc(self, msg):
        """
        Process Messages from RPC. msg is built as a dict containing the command as the key and the payload as value.
        :param msg:
        :return:
        """
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
        #TODO

        # add new job
        if "add_job_new" in msg:
            LOG.debug("Creating New Job")
            print(pymJob.__subclasses__())
            if self.createjob(msg["add_job_new"]):
                LOG.debug("Succesfully Created New Job")
                reply["add_job_new"] = 1

        #TODO

        # pickle existing job
        #TODO

        # unpickle transfered job
        #TODO

        """
        Journal related commands
        """

        #TODO

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
            LOG.debug("Received Shutdown Signal")
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