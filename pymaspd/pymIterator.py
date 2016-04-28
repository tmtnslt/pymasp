import logging
import json
import pymJournal
from pymJob import *
from pymParameter import *

class pymIterator(pymJob):
    __doc__ =  "iterates over one parameter's range and does one job for each parameter"

    def __init__(self, jobid):
        super().__init__(jobid)
        self.parameter = None
        self.job = None
        self.running = None
        self.use_altunit = False

    def ismutable(self):
        if isinstance(self.job, pymJob):
            return self.job.ismutable()
        return False

    def appendjob(self, job):
        if self.ismutable():
            return self.job.appendjob(job)
        return False

    def deletejob(self, job):
        if self.ismutable():
            return self.job.deletejob(job)
        return False

    def insertjobafter(self, job, refjob):
        if self.ismutable():
            return self.job.insertjobafter(job, refjob)
        return False

    def movejob(self, job, n):
        if self.ismutable():
            return self.job.movejob(job, n)
        return False

    def isrunning(self):
        return self.running

    def description(self):
        if isinstance(self.parameter, pymParameter):
            param_name = self.parameter.description()
            if self.use_altunit:
                param_unit = str(self.parameter.altUnitString)
            else:
                param_unit = str(self.parameter.natUnitString)
        else:
            param_name = "(tbd)"
            param_unit = ""
        (param_min, param_max) = self.get_range()
        desc = "Vary " + param_name + " from " + str(param_min) + " to " + str(param_max) + " " + param_unit + " (" + str(self.get_steps()) + " Steps)"
        return (desc, [self.parameter, self.job])

    def updatejob(self, settings_dict):
        """
        Update settings of this job according to values supplied by the dictionary.
        The dictionary is expected to supply values to the keys as give by the getsettings() method.
        :param settings_dict:
        :return: True if any settings have been changed successfully
        """
        res = False
        if 'parameter' in settings_dict:
            if settings_dict['parameter'] == type(self.parameter):
                logging.debug("Same class for Parameter was supplied as already set, don't change anything")
            else:
                # explicitly delete parameter
                del self.parameter
                # create new instance of parameter out of factory
                self.parameter = pynParameterFactory.createParameter(settings_dict['parameter'])
                self.set_range(self.range_min,self.range_max,False) # recheck sanity of range and stepsize
                self.set_stepsize(self.stepsize, False) # internal values are always natural unit!
                res = True

        if 'job' in settings_dict:
            if settings_dict['job'] == type(self.job):
                logging.debug("Same class for Job was supplied as already set, don't change anything")
            else:
                # explicitly deleter job
                del self.job
                # create new instance of job out of factory
                self.job = pynJobFactory.createJob(settings_dict['job'])
                res = True

        if 'parameterSettings' in settings_dict:
            # do we need to sanitize anything?
            if isinstance(self.parameter, pymParameter):
                if self.parameter.updatesettings(settings_dict['parameterSettings']):
                    res = True

        if 'jobSettings' in settings_dict:
            # do we need to sanitize anything?
            if isinstance(self.job, pymJob):
                if self.job.updatejob(settings_dict['jobSettings']):
                    res = True

        if 'useAltUnit' in settings_dict:
            # change the alternative unit setting first!
            if isinstance(self.parameter, pymParameter):
                if self.parameter.hasAltUnit:
                    use_altunit = settings_dict['useAltUnit']
                    res = True

        if 'range' in settings_dict:
            if self.set_range(settings_dict['range'][0], settings_dict['range'][1], self.use_altunit):
                res = True

        if 'steps' in settings_dict:
            if self.set_steps(settings_dict['steps']):
                res = True

        if 'stepsize' in settings_dict:
            if self.set_stepsize(settings_dict['stepsize'], self.use_altunit):
                res = True

        return res


    def getsettings(self):
        """
        Returns a dictionary with settings to be set. Each key includes a own dictionary with current values (current),
        expected type (type), optional human readable hint (hint) and flag if parameter is read only (ro)
        :return: settings_dict
        """
        return {
            'parameter': {'current': self.parameter, 'type': 'pynParameter', 'hint': 'Parameter which will be varried', 'ro': False},
            'job': {'current': self.job, 'type': 'pynJob', 'hint': 'subjob which will be run for each parameter', 'ro': False},
            'range': {'current': self.get_range(), 'type': 'touple', 'hint': 'Range of parameter', 'ro': False},
            'steps': {'current': self.get_steps(), 'type': 'int', 'hint': 'Steps in run', 'ro': False},
            'stepsize': {'current': self.get_stepsize(), 'type': 'double', 'hint': 'Stepsize', 'ro': False},
            'hasAltUnit':  {'current': self.get_altunitavailable(), 'type': 'string', 'hint': 'Is there an alternative unit available?', 'ro': True},
            'useAltUnit':  {'current': self.use_altunit, 'type': 'bool', 'hint': 'Give range or stepsize in alternative unit?', 'ro': False},
            'jobSettings': {'current': self.get_subjobsettings(), 'type' : 'settings_dict', 'hint': 'Settings of the attached subjob', 'ro': False},
            'parameterSettings': {'current': self.get_parametersettings(), 'type' : 'settings_dict', 'hint': 'Settings of the attached parameter', 'ro': False}
        }

    def get_altunitavailable(self):
        if isinstance(self.parameter, pymParameter):
            if self.parameter.hasAltUnit:
                return self.parameter.altUnitString
        return ""

    def get_subjobsettings(self):
        if isinstance(self.job, pymJob):
            return self.job.getsettings()
        return None

    def get_parametersettings(self):
        if isinstance(self.parameter, pymParameter):
            return self.parameter.getsettings()
        return None


    def set_range(self, range_min, range_max, altUnit= False):
        """
        Sets the range you want to vary the parameter in:
        :param range_min:
        :param range_max:
        :param altUnit:
        :return:
        """
        if isinstance(self.parameter, pymParameter):
            if altUnit and self.parameter.hasAltUnit:
                self.range_min = self.parameter.sanityCheckValue(self.parameter.convertFromAlt(range_min, self.channel), self.channel)
                self.range_max = self.parameter.sanityCheckValue(self.parameter.convertFromAlt(range_max, self.channel), self.channel)
            else:
                self.range_min = self.parameter.sanityCheckValue(range_min, self.channel)
                self.range_max = self.parameter.sanityCheckValue(range_max, self.channel)
        else:
            self.range_min = range_min
            self.range_max = range_max

    def get_range(self):
        if self.use_altunit and isinstance(self.parameter, pymParameter):
            return (self.parameter.convertToAlt(self.range_min),self.parameter.convertToAlt(self.range_max))
        else:
            return (self.range_min, self.range_max)

    def set_steps(self, steps):
        """
        Set the steps you want to vary the parameter with
        :param steps:
        :return:
        """

        self.stepsize = self.parameter.getClosestParameterStep((self.range_max-self.range_min)/steps, self.channel)

    def get_steps(self):
        return len(range(self.range_min, self.range_max, self.stepsize))


    def set_stepsize(self, stepsize, altUnit=False):
        """
        Set the stepsize you want to vary the parameter with
        :param stepsize:
        :return:
        """
        if isinstance(self.parameter, pymParameter):
            if altUnit:
                self.stepsize = self.parameter.getClosestParameterStep(self.parameter.convertFromAlt(stepsize), self.channel)
            else:
                self.stepsize = self.parameter.getClosestParameterStep(stepsize, self.channel)
        else:
            self.stepsize = stepsize

    def get_stepsize(self):
        if self.use_altunit and self.get_altunitavailable():
            return self.parameter.convertToAlt(self.stepsize)



    def set_channel(self, channel):
        self.channel = channel


    def get_channel(self):
        return self.channel

    async def process_late_collection(self, result, late_param):
        if callable(result):
            # last iteration returned a callable to do a lazy collection of the data
            # collect data now and append its journal_id to our list
            result = await result()
        if result:
            # result is not None, so no error and not the first iteration.
            pymJournal.assign_parameter_to_job(result, late_param, self.journal_id)
        return result



    async def run(self, parent_id):
        """
        Run the iterator as a asyncio coroutine
        :return:
        """
        self.running = True
        # calculate what our parameter range will actually be
        parameter_list = range(self.range_min, self.range_max, self.stepsize)
        # obtain a journal_id
        self.journal_id = pymJournal.add_job(parent_id, self.description()[0], json.dumps(
            {'parameter_list':parameter_list,
             'range':(self.range_min,self.range_max),
             'stepsize':self.stepsize,
             'steps':len(parameter_list)}))
        result = None
        res = []
        last_x = None
        for x in parameter_list:
            # start movement of the parameter
            await self.parameter.go(x,self.channel,wait=False)
            #do late collection of data
            res.append(self.process_late_collection(result, last_x))
            # wait until parameter is where we want it to be
            await self.parameter.go(x, self.channel, wait=True)
            # now do the job for this iteration
            result = await self.job.run(self.journal_id)
            last_x = x

        # last late collection
        res.append(self.process_late_collection(result, last_x))
        # our job is done
        self.running = False
        # update our job entry in the database TODO refactor this to be unified
        pymJournal.update_job(self.journal_id, self.description()[0], json.dump(
            {'parameter_list':parameter_list,
             'assoc_list':res,
             'range':(self.range_min,self.range_max),
             'stepsize':self.stepsize,
             'steps':len(parameter_list)}))
        return self.journal_id


