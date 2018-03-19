#!/usr/bin/python
"""
\brief Container for the settings of a simulation run.

\author Thomas Watteyne <watteyne@eecs.berkeley.edu>
\author Kazushi Muraoka <k-muraoka@eecs.berkeley.edu>
\author Nicola Accettura <nicola.accettura@eecs.berkeley.edu>
\author Xavier Vilajosana <xvilajosana@eecs.berkeley.edu>
"""

#============================ logging =========================================

import logging
class NullHandler(logging.Handler):
    def emit(self, record):
        pass
log = logging.getLogger('SimSettings')
log.setLevel(logging.ERROR)
log.addHandler(NullHandler())

#============================ imports =========================================

import os

#============================ defines =========================================

#============================ body ============================================

class SimSettings(object):

    #===== start singleton
    _instance      = None
    _init          = False

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(SimSettings,cls).__new__(cls, *args, **kwargs)
        return cls._instance
    #===== end singleton

    def __init__(self, cpuID=None, runNum=None, failIfNotInit=False, **kwargs):

        if failIfNotInit and not self._init:
            raise EnvironmentError('SimSettings singleton not initialized.')

        #===== start singleton
        if self._init:
            return
        self._init = True
        #===== end singleton

        # store params
        self.cpuID                          = cpuID
        self.runNum                         = runNum

        self.__dict__.update(kwargs)

    def setStartTime(self,startTime):
        self.startTime       = startTime

    def setCombinationKeys(self,combinationKeys):
        self.combinationKeys = combinationKeys

    def getOutputFile(self):
        # directory
        dirname   = os.path.join(
            self.simDataDir,
            '_'.join(['{0}_{1}'.format(k,getattr(self,k)) for k in self.combinationKeys]),
        )
        if not os.path.exists(dirname):
            os.makedirs(dirname)

        # file
        if self.cpuID==None:
            tempname         = 'output.dat'
        else:
            tempname         = 'output_cpu{0}.dat'.format(self.cpuID)
        datafilename         = os.path.join(dirname,tempname)

        return datafilename

    def destroy(self):
        self._instance       = None
        self._init           = False

        # This destroy method doesn't destroy the instance in fact. Since I'm
        # not sure how to destroy the singleton instance from its instance
        # method, just remove relevant instance attributes so that tests
        # get happy.
        if hasattr(self, 'topology'):
            del self.topology
        if hasattr(self, 'numFragments'):
            del self.numFragments
        if hasattr(self, 'cascadingScheduling'):
            del self.cascadingScheduling
        if hasattr(self, 'schedulingMode'):
            del self.schedulingMode
        if hasattr(self, 'numReassQueue'):
            del self.numReassQueue
        if hasattr(self, 'maxVRBEntryNum'):
            del self.maxVRBEntryNum
        if hasattr(self, 'optFragmentForwarding'):
            del self.optFragmentForwarding
