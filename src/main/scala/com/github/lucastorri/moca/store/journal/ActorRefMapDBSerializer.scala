package com.github.lucastorri.moca.store.journal

import java.io.{DataInput, DataOutput}

import akka.actor.ActorRef
import akka.serialization.{JavaSerializer, Serialization}
import org.mapdb.Serializer

class ActorRefMapDBSerializer extends Serializer[ActorRef] with Serializable {

  override def serialize(out: DataOutput, value: ActorRef): Unit =
    out.writeUTF(Serialization.serializedActorPath(value))

  override def deserialize(in: DataInput, available: Int): ActorRef =
    JavaSerializer.currentSystem.value.provider.resolveActorRef(in.readUTF())

}
