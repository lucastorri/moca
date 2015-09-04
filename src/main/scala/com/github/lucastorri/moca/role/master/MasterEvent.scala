package com.github.lucastorri.moca.role.master

sealed trait MasterEvent
case object MasterUp extends MasterEvent
case object MasterDown extends MasterEvent
