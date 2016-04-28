from pymParameter import *
import logging
import asyncio

from time import sleep

class dummy_parameter(pymParameter):
    def description(self):
        return ("Dummy Stage", None)


    MIN_POSITION = -10000
    MAX_POSITION = 10000

    MINIMAL_STEP = 1
    STEP_MULTIPLICATOR = 1

    RAW_UNIT = "mu"

    HAS_ALTERNATIVE_UNIT = True
    ALTERNATIVE_UNIT = "fs"
    ALT_UNIT_MULTIPLIER = 2 # 2 for double pass on stage

    NUM_CHANNELS = 1


    def __init__(self):
        super().__init__()

        # we start initialized
        self.initialized = True

        self.home_value = 0
        self.current_value = 0
        self.hasAltUnit = HAS_ALTERNATIVE_UNIT
        self.num_channels = NUM_CHANNELS

        self.altUnitString = "fs"
        self.natUnitString = "mu"

    def sanityCheckValue(self, value, channel):
        if value < MIN_POSITION:
            return MIN_POSITION
        if value > MAX_POSITION:
            return MAX_POSITION



    def getClosestParameterStep(self, step, channel):
        """
        return next reasonable stepsize for parameter. Also acts as a sanity check for steps
        """
        if (step % MINIMAL_STEP):
            return (step // MINIMAL_STEP) * MINIMAL_STEP
        else:
            return step


    def convertToAlt(self, value, channel):
        """
        calculate alternative unit from raw
        """
        return 1/0.299792458*value*ALT_UNIT_MULTIPLIER

    def convertFromAlt(self, value, channel):
        """
        calculate raw from alternative unit
        """
        return value/ALT_UNIT_MULTIPLIER/(1/0.299792458)

    async def set(self, value, channel, wait=True):
        """
        set parameter on channel to value, wait for completion on default
        :param value:
        :param channel:
        :param wait:
        :return:
        """

        logging.debug("Dummy Stage received move command")
        # check if the value is sane
        if not self.sanityCheckValue(value): raise pynParameterException("Value out of sane range")


        if (wait):
            # do a blocking move
            sleep(100) #simulate some walk
            self.current_value = value
            logging.debug("Dummy Stage reached target position")
            return True
        else:
            # async movement of a stepper, not implemeneted
            await asyncio.sleep(100)
            self.current_value = value
            logging.debug("Dummy Stage reached target position (asyncio)")
            return True



    def get_current_value(self):
        return self.current_value