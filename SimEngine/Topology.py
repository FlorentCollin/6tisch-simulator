#!/usr/bin/python
'''
\brief Wireless network topology creator.

\author Thomas Watteyne <watteyne@eecs.berkeley.edu>
\author Kazushi Muraoka <k-muraoka@eecs.berkeley.edu>
\author Nicola Accettura <nicola.accettura@eecs.berkeley.edu>
\author Xavier Vilajosana <xvilajosana@eecs.berkeley.edu>
'''

#============================ logging =========================================

import logging
class NullHandler(logging.Handler):
    def emit(self, record):
        pass
log = logging.getLogger('Topology')
log.setLevel(logging.ERROR)
log.addHandler(NullHandler())

#============================ imports =========================================

import random
import math

import SimSettings

#============================ defines =========================================

#============================ body ============================================

class Topology(object):
    
    TWO_DOT_FOUR_GHZ         = 2400000000   # Hz
    PISTER_HACK_LOWER_SHIFT  = 40           # -40 dB
    SPEED_OF_LIGHT           = 299792458    # m/s
    
    STABLE_RSSI              = -93          # dBm, corresponds to PDR = 0.5
    STABLE_NEIGHBORS         = 3
    def __init__(self, motes):
        
        # store params
        self.motes           = motes
        
        # local variables
        self.settings        = SimSettings.SimSettings()
    
    #======================== public ==========================================
    
    def createTopology(self):
        '''
        Create a topology in which all nodes have at least STABLE_NEIGHBORS link 
        with enough RSSI.
        If the mote does not have STABLE_NEIGHBORS links with enough RSSI, 
        reset the location of the mote.
        '''
        
        # find DAG root
        dagRoot = None
        for mote in self.motes:
            if mote.id==0:
                mote.role_setDagRoot()
                dagRoot = mote
        assert dagRoot
        
        # put DAG root at center of area
        dagRoot.setLocation(
            x = self.settings.squareSide/2,
            y = self.settings.squareSide/2
        )
        
        # reposition each mote until it is connected
        connectedMotes = [dagRoot]
        for mote in self.motes:
            if mote in connectedMotes:
                continue
            
            connected = False
            while not connected:
                # pick a random location
                mote.setLocation(
                    x = self.settings.squareSide*random.random(),
                    y = self.settings.squareSide*random.random()
                )
                
                numStableNeighbors = 0
                
                # count number of neighbors with sufficient RSSI
                for cm in connectedMotes:
                    
                    rssi = self._computeRSSI(mote, cm)
                    mote.setRSSI(cm, rssi)
                    cm.setRSSI(mote, rssi)
                    
                    if rssi>self.STABLE_RSSI:
                        numStableNeighbors += 1

                # make sure it is connected to at least STABLE_NEIGHBORS motes 
                # or connected to all the currently deployed motes when the number of deployed motes 
                # are smaller than STABLE_NEIGHBORS
                if numStableNeighbors >= self.STABLE_NEIGHBORS or numStableNeighbors == len(connectedMotes):
                    connected = True
            
            connectedMotes += [mote]
        
        # for each mote, compute PDR to each neighbors
        for mote in self.motes:
            for m in self.motes:
                if mote==m:
                    continue
                if mote.getRSSI(m)>mote.minRssi:
                    pdr = self._computePDR(mote,m)
                    mote.setPDR(m,pdr)
                    m.setPDR(mote,pdr)
        
        # print topology information
        '''
        for mote in self.motes:
            for neighbor in self.motes:
                try:
                    distance = self._computeDistance(mote,neighbor)
                    rssi     = mote.getRSSI(neighbor)
                    pdr      = mote.getPDR(neighbor)
                except KeyError:
                    pass
                else:
                    print "mote = {0:>3}, neigh = {1:<3}, dist = {2:>3}m, rssi = {3:>3}dBm, pdr = {4:.3f}%".format(
                        mote.id,
                        neighbor.id,
                        int(distance),
                        int(rssi),
                        100*pdr
                    )
        '''
    
    #======================== private =========================================
    
    def _computeRSSI(self,mote,neighbor):
        ''' computes RSSI between any two nodes (not only neighbor) according to Pister hack model'''
        
        # distance in m
        distance = self._computeDistance(mote,neighbor)
        
        # sqrt and inverse of the free space path loss
        fspl = (self.SPEED_OF_LIGHT/(4*math.pi*distance*self.TWO_DOT_FOUR_GHZ))
        
        # simple friis equation in Pr=Pt+Gt+Gr+20log10(c/4piR)
        pr = mote.txPower + mote.antennaGain + neighbor.antennaGain + (20*math.log10(fspl))
        
        # according to the receiver power (RSSI) we can apply the Pister hack model.
        mu = pr-self.PISTER_HACK_LOWER_SHIFT/2 #chosing the "mean" value
    
        # the receiver will receive the packet with an rssi distributed in a gaussian between friis and friis -40
        #rssi=random.gauss(mu,self.PISTER_HACK_LOWER_SHIFT/2)
    
        # the receiver will receive the packet with an rssi uniformly distributed between friis and friis -40
        rssi = mu + random.uniform(-self.PISTER_HACK_LOWER_SHIFT/2, self.PISTER_HACK_LOWER_SHIFT/2)
        
        return rssi
    
    def _computePDR(self,mote,neighbor):
        ''' computes pdr to neighbor according to RSSI'''
        
        rssi        = mote.getRSSI(neighbor)
        return self.rssiToPdr(rssi)
    
    @classmethod
    def rssiToPdr(self,rssi):

        # local variables
        settings   = SimSettings.SimSettings()        
        minRssi    = settings.sensitivity - settings.waterfallRisingBand
        if   rssi<=minRssi:
            pdr    = 0.0
        elif minRssi<rssi and rssi<settings.sensitivity:
            pdr    = (rssi-minRssi)*(1.0/float(settings.waterfallRisingBand))
        elif rssi>settings.sensitivity:
            pdr    = 1.0
        
        assert pdr>=0.0
        assert pdr<=1.0
        
        return pdr
    
    def _computeDistance(self,mote,neighbor):
        
        return 1000*math.sqrt(
            (mote.x - neighbor.x)**2 +
            (mote.y - neighbor.y)**2
        )
