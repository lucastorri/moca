package com.github.lucastorri.moca.store.serialization

import akka.actor.{ExtendedActorSystem, ActorSystem}

import scala.reflect.ClassTag

class KryoSerializerService(system: ActorSystem) extends SerializerService {

  private val extendedSystem = system.asInstanceOf[ExtendedActorSystem]

  override def create[T : ClassTag]: Serializer[T] = new KryoSerializer[T](extendedSystem)

}
