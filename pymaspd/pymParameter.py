from pymJob import *
import asyncio

class pymParameter:
    """
    pynParameter is the abstraction of any parameter we might want to vary in an experiment (eg your X-axis later)
    such as a motorized stage, a motorized iris, a shutter, etc.
    a parameter might have different channels (such as two axis)
    An important difference of a parameter to a job is, that a parameter can not act on its own. The method to do the
    main action of a parameter is set(value, channel, [wait]) instead of run()
    """
    def description(self):
        raise NotImplementedError

    @classmethod
    def create_param(cls):
        return cls()

    def __init__(self):
        # do we have more than one channel?
        self.num_channels = 1

        # does the parameter start initialized or do we need to do stuff?
        self.initialized = True

        # does your parameter have a convenient unit you might want to be able to use? Eg ps instead of mm on a motorized stage?
        self.hasAltUnit = False
        self.altUnitString = ""
        self.natUnitString = ""

    def initialize(self):
        if self.initialized:
            return True
        else:
            return self._initialize()

    def _initialize(self):
        # private init function
        pass

    def sanityCheckValue(self, value, channel):
        """
        check whether a supplied value is inside a sane range for Device/Hardware
        """
        pass

    def getClosestParameterStep(self, step, channel):
        """
        return next reasonable stepsize for parameter. Also acts as a sanity check for steps
        """
        pass


    def convertToAlt(self, value, channel):
        """
        calculate alternative unit from raw
        """
        pass

    def convertFromAlt(self, value, channel):
        """
        calculate raw from alternative unit
        """
        pass

    def updatesettings(self, settings_dict):
        pass

    def getsettings(self):
        pass

    async def set(self, value, channel, wait=True):
        """
        set parameter on channel to value, wait for completion on default
        :param value:
        :param channel:
        :param wait:
        :return:
        """
        pass


class pynParameterFactory:
    @staticmethod
    def createParameter(classid):
        for params in pymParameter.__subclasses__(): # potentially harmful if one can manipulate subclasses to load malicious jobs.
            if classid == params.__name__:
                return params.create_param()




class pynParameterException(Exception):
    pass
