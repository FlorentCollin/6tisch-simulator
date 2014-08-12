#!/usr/bin/python

'''
 @authors:
       Thomas Watteyne    <watteyne@eecs.berkeley.edu>    
'''

import logging
class NullHandler(logging.Handler):
    def emit(self, record):
        pass
log = logging.getLogger('SimGui')
log.setLevel(logging.ERROR)
log.addHandler(NullHandler())

import threading
import Tkinter

import ScheduleFrame
import TopologyFrame
import ActionFrame
import StatsFrame

class SimGui(Tkinter.Tk):
    
    def __init__(self):
        
        # log
        log.info("SimGui starting")
        
        # store params
        
        # local variables
        self.dataLock        = threading.Lock()
        self._selectedCell   = None
        self._selectedMote   = None
        self._selectedLink   = None
        
        # initialize parent class
        Tkinter.Tk.__init__(self)
        
        # assign a title to this window
        self.title("6tsch simulator")
        
        # set a function to call when "x" close button is pressed
        self.protocol('WM_DELETE_WINDOW',self._closeWindow)
        
        # this window can not be resized
        self.resizable(0,0)
        
        # create frames
        self.scheduleFrame = ScheduleFrame.ScheduleFrame(self)
        self.scheduleFrame.grid(row=0,column=0,columnspan=2)
        self.topologyFrame = TopologyFrame.TopologyFrame(self)
        self.topologyFrame.grid(row=1,column=0,rowspan=2)
        self.actionFrame = ActionFrame.ActionFrame(self)
        self.actionFrame.grid(row=1,column=1)
        self.statsFrame = StatsFrame.StatsFrame(self)
        self.statsFrame.grid(row=2,column=1)
        
        # start GUI
        self.mainloop()
    
    #======================== public ==========================================
    
    @property
    def selectedCell(self):
        with self.dataLock:
            return self._selectedCell
    
    @selectedCell.setter
    def selectedCell(self, value):
        with self.dataLock:
            self._selectedCell = value
    
    @property
    def selectedMote(self):
        with self.dataLock:
            return self._selectedMote
    
    @selectedMote.setter
    def selectedMote(self, value):
        with self.dataLock:
            self._selectedMote = value
    
    @property
    def selectedLink(self):
        with self.dataLock:
            return self._selectedLink
    
    @selectedLink.setter
    def selectedLink(self, value):
        with self.dataLock:
            self._selectedLink = value
    
    #======================== private =========================================
    
    def _closeWindow(self):
        self.scheduleFrame.quit()
        self.topologyFrame.quit()
        self.statsFrame.quit()
        self.destroy()
