package com.github.lucastorri.moca.role.master.scheduler

trait TaskSchedulerCreator {

  def apply(): TaskScheduler
  
}
