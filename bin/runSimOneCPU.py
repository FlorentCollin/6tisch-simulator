#!/usr/bin/python
'''
\brief Entry point to start a simulation.

A number of command-line parameters are available to modify the simulation
settings. Use '--help' for a list of them.

\author Thomas Watteyne <watteyne@eecs.berkeley.edu>
\author Kazushi Muraoka <k-muraoka@eecs.berkeley.edu>
\author Nicola Accettura <nicola.accettura@eecs.berkeley.edu>
\author Xavier Vilajosana <xvilajosana@eecs.berkeley.edu>
'''

#============================ adjust path =====================================

import os
import sys
if __name__=='__main__':
    here = sys.path[0]
    sys.path.insert(0, os.path.join(here, '..'))

#============================ logging =========================================

import logging
class NullHandler(logging.Handler):
    def emit(self, record):
        pass
log = logging.getLogger('BatchSim')
log.setLevel(logging.ERROR)
log.addHandler(NullHandler())

#============================ imports =========================================

import time
import itertools
import logging.config
import argparse
import threading

from SimEngine     import SimEngine,   \
                          SimSettings, \
                          SimStats
from SimGui        import SimGui

#============================ defines =========================================

#============================ helpers =========================================

def parseCliOptions():
    
    parser = argparse.ArgumentParser()
    # sim
    parser.add_argument('--gui',
        dest       = 'gui',
        action     = 'store_true',
        default    = False,
        help       = '[sim] Display the GUI.',
    )
    parser.add_argument( '--cpuID',
        dest       = 'cpuID',
        type       = int,
        default    = None,
        help       = '[sim] id of the CPU running this simulation (for batch).',
    )
    parser.add_argument( '--numRuns',
        dest       = 'numRuns',
        type       = int,
        default    = 2,
        help       = '[sim] Number of simulation runs.',
    )
    parser.add_argument( '--numCyclesPerRun',
        dest       = 'numCyclesPerRun',
        type       = int,
        default    = 100,
        help       = '[simulation] Duration of a run, in slotframes.',
    )
    parser.add_argument('--simDataDir',
        dest       = 'simDataDir',
        type       = str,
        default    = 'simData',
        help       = '[simulation] Simulation log directory.',
    )
    # topology
    parser.add_argument( '--numMotes',
        dest       = 'numMotes',
        nargs      = '+',
        type       = int,
        default    = [50],
        help       = '[topology] Number of simulated motes.',
    )
    parser.add_argument( '--squareSide',
        dest       = 'squareSide',
        type       = float,
        default    = 2.000,
        help       = '[topology] Side of the deployment area (km).',
    )
    parser.add_argument('--fullyMeshed',
        dest       = 'fullyMeshed',
        nargs      = '+',
        type       = int,
        default    = 0,
        help       =' [topology] 1 to enable fully meshed network.',
    )
    # join process
    parser.add_argument('--withJoin',
        dest       = 'withJoin',
        nargs      = '+',
        type       = int,
        default    = 0,
        help       =' [join process] 1 to enable join process.',
    )
    parser.add_argument( '--joinNumExchanges',
        dest       = 'joinNumExchanges',
        nargs      = '+',
        type       = int,
        default    = 2,
        help       = '[join process] Number of exchanges needed to complete join process.',
    )
    parser.add_argument( '--joinAttemptTimeout',
        dest       = 'joinAttemptTimeout',
        type       = float,
        default    = 60.0,
        help       = '[join process] Timeout to attempt join process (s).',
    )
    # app
    parser.add_argument( '--pkPeriod',
        dest       = 'pkPeriod',
        nargs      = '+',
        type       = float,
        default    = 10,
        help       = '[app] Average period between two data packets (s).',
    )
    parser.add_argument( '--pkPeriodVar',
        dest       = 'pkPeriodVar',
        type       = float,
        default    = 0.05,
        help       = '[app] Variability of pkPeriod (0.00-1.00).',
    )
    parser.add_argument( '--burstTimestamp',
        dest       = 'burstTimestamp',
        nargs      = '+',
        type       = float,
        default    = None,
        help       = '[app] Timestamp when the burst happens (s).',
    )
    parser.add_argument( '--numPacketsBurst',
        dest       = 'numPacketsBurst',
        nargs      = '+',
        type       = int,
        default    = None,
        help       = '[app] Number of packets in a burst, per node.',
    )
    parser.add_argument('--downwardAcks',
        dest       = 'downwardAcks',
        nargs      = '+',
        type       = int,
        default    = 0,
        help       = '[app] 1 to enable downward end-to-end ACKs.',
    )
    # rpl
    parser.add_argument( '--dioPeriod',
        dest       = 'dioPeriod',
        type       = float,
        default    = 10.0,
        help       = '[rpl] DIO period (s).',
    )
    parser.add_argument( '--daoPeriod',
        dest       = 'daoPeriod',
        type       = float,
        default    = 60.0,
        help       = '[rpl] DAO period (s).',
    )
    # otf
    parser.add_argument( '--otfThreshold',
        dest       = 'otfThreshold',
        nargs      = '+',
        type       = int,
        default    = 1,
        help       = '[otf] OTF threshold (cells).',
    )
    parser.add_argument( '--msfHousekeepingPeriod',
        dest       = 'msfHousekeepingPeriod',
        type       = float,
        default    = 60.0,
        help       = '[msf] MSF HOUSEKEEPINGCOLLISION_PERIOD parameter (s).',
    )
    # msf
    parser.add_argument( '--msfMaxNumCells',
        dest       = 'msfMaxNumCells',
        nargs      = '+',
        type       = int,
        default    = 16,
        help       = '[msf] MSF MAX_NUMCELLS parameter.',
    )
    parser.add_argument( '--msfLimNumCellsUsedHIGH',
        dest       = 'msfLimNumCellsUsedHigh',
        nargs      = '+',
        type       = int,
        default    = 12,
        help       = '[msf] MSF LIM_NUMCELLSUSED_HIGH parameter.',
    )
    parser.add_argument( '--msfLimNumCellsUsedLOW',
        dest       = 'msfLimNumCellsUsedLow',
        nargs      = '+',
        type       = int,
        default    = 4,
        help       = '[msf] MSF LIM_NUMCELLSUSED_LOW parameter.',
    )
    parser.add_argument( '--msfNumCellsToAddOrRemove',
        dest       = 'msfNumCellsToAddOrRemove',
        nargs      = '+',
        type       = int,
        default    = 1,
        help       = '[msf] MSF number of cells to add/remove when 6P is triggered.',
    )
    # sixtop
    parser.add_argument( '--sixtopMessaging',
        dest       = 'sixtopMessaging',
        type       = int,
        default    = 1,
        help       = '[6top] 1 to enable 6top messaging, 0 to enable 6top GOD mode.',
    )
    parser.add_argument('--sixtopNoRemoveWorstCell',
        dest       = 'sixtopNoRemoveWorstCell',
        nargs      = '+',
        type       = int,
        default    = 0,
        help       = '[6top] 1 to remove random cell, not worst.',
    )
    # tsch
    parser.add_argument( '--slotDuration',
        dest       = 'slotDuration',
        type       = float,
        default    = 0.010,
        help       = '[tsch] Duration of a timeslot (s).',
    )
    parser.add_argument( '--slotframeLength',
        dest       = 'slotframeLength',
        nargs      = '+',
        type       = int,
        default    = 101,
        help       = '[tsch] Number of timeslots in a slotframe.',
    )
    parser.add_argument('--beaconPeriod',
        dest       = 'beaconPeriod',
        nargs      = '+',
        type       = float,
        default    = 2.0,
        help       = '[tsch] Enhanced Beacon period (s).',
    )
    # Bayesian broadcast algorithm
    parser.add_argument('--bayesianBroadcast',
        dest       = 'bayesianBroadcast',
        type       = int,
        default    = 1,
        help       = '[tsch] Enable Bayesian broadcast algorithm.',
    )
    parser.add_argument('--beaconProbability',
        dest       = 'beaconProbability',
        nargs      = '+',
        type       = float,
        default    = 0.33,
        help       = '[tsch] Beacon probability with Bayesian broadcast algorithm.',
    )
    parser.add_argument('--dioProbability',
        dest       = 'dioProbability',
        nargs      = '+',
        type       = float,
        default    = 0.33,
        help       = '[tsch] DIO probability with Bayesian broadcast algorithm.',
    )
    # phy
    parser.add_argument( '--numChans',
        dest       = 'numChans',
        type       = int,
        default    = 16,
        help       = '[phy] Number of frequency channels.',
    )
    parser.add_argument( '--minRssi',
        dest       = 'minRssi',
        type       = int,
        default    = -97,
        help       = '[phy] Mininum RSSI with positive PDR (dBm).',
    )
    parser.add_argument('--noInterference',
        dest       = 'noInterference',
        nargs      = '+',
        type       = int,
        default    = 0,
        help       = '[phy] Disable interference model.',
    )
    
    options        = parser.parse_args()
    
    return options.__dict__

def printOrLog(simParam,output):
    if simParam['cpuID']!=None:
        with open('cpu{0}.templog'.format(simParam['cpuID']),'w') as f:
            f.write(output)
    else:
        print output

def runSims(options):
    
    # record simulation start time
    simStartTime   = time.time()
    
    # compute all the simulation parameter combinations
    combinationKeys     = sorted([k for (k,v) in options.items() if type(v)==list])
    simParams           = []
    for p in itertools.product(*[options[k] for k in combinationKeys]):
        simParam = {}
        for (k,v) in zip(combinationKeys,p):
            simParam[k] = v
        for (k,v) in options.items():
            if k not in simParam:
                simParam[k] = v
        simParams      += [simParam]
    
    # run a simulation for each set of simParams
    for (simParamNum,simParam) in enumerate(simParams):
        
        # record run start time
        runStartTime = time.time()
        
        # run the simulation runs
        for runNum in xrange(simParam['numRuns']):
            
            # print
            output  = 'parameters {0}/{1}, run {2}/{3}'.format(
               simParamNum+1,
               len(simParams),
               runNum+1,
               simParam['numRuns']
            )
            printOrLog(simParam,output)
            
            # create singletons
            settings         = SimSettings.SimSettings(**simParam)
            settings.setStartTime(runStartTime)
            settings.setCombinationKeys(combinationKeys)
            simengine        = SimEngine.SimEngine(runNum)
            simstats         = SimStats.SimStats(runNum)
            
            # start simulation run
            simengine.start()
            
            # wait for simulation run to end
            simengine.join()
            
            # destroy singletons
            simstats.destroy()
            simengine.destroy()
            settings.destroy()
        
        # print
        output  = 'simulation ended after {0:.0f}s.'.format(time.time()-simStartTime)
        printOrLog(simParam,output)

#============================ main ============================================

def main():
    # initialize logging
    logging.config.fileConfig('logging.conf')
    
    # parse CLI options
    options        = parseCliOptions()
    
    if options['gui']:
        # create the GUI
        gui        = SimGui.SimGui()
        
        # run simulations (in separate thread)
        simThread  = threading.Thread(target=runSims,args=(options,))
        simThread.start()
        
        # start GUI's mainloop (in main thread)
        gui.mainloop()
    else:        
        # run the simulations
        runSims(options)

if __name__=="__main__":
    main()
