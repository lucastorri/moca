package com.github.lucastorri.moca.store.serialization

import akka.actor.{ExtendedActorSystem, ActorRef}
import akka.serialization.Serialization
import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer => KryoSerializer}

case class ActorRefKryoSerializer(system: ExtendedActorSystem) extends KryoSerializer[ActorRef] {

  override def write(kryo: Kryo, out: Output, ref: ActorRef): Unit =
    out.writeString(Serialization.serializedActorPath(ref))

  override def read(kryo: Kryo, in: Input, typ: Class[ActorRef]): ActorRef =
    system.provider.resolveActorRef(in.readString())

}
