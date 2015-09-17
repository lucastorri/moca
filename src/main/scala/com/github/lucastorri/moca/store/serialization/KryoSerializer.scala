package com.github.lucastorri.moca.store.serialization

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import akka.actor.{ActorRef, ExtendedActorSystem}
import akka.serialization.Serialization
import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer => LibKryoSerializer}
import org.objenesis.strategy.StdInstantiatorStrategy

import scala.reflect.ClassTag

class KryoSerializer[T](system: ExtendedActorSystem)(implicit tag: ClassTag[T]) extends Serializer[T] {

  private def kryo: Kryo = {
    val k = new Kryo()
    k.setRegistrationRequired(false)
    k.setInstantiatorStrategy(new StdInstantiatorStrategy)

    k.addDefaultSerializer(classOf[ActorRef], ActorRefKryoSerializer(system))

    k
  }

  def serialize(t: T): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    val kryoOut = new Output(out)
    kryo.writeObject(kryoOut, t)
    kryoOut.flush()
    kryoOut.close()
    val buf = out.toByteArray
    out.close()
    buf
  }

  def deserialize(buf: Array[Byte]): T = {
    val in = new ByteArrayInputStream(buf)
    val kryoIn = new Input(in)
    val r = kryo.readObject(kryoIn, tag.runtimeClass).asInstanceOf[T]
    kryoIn.close()
    in.close()
    r
  }

}

case class ActorRefKryoSerializer(system: ExtendedActorSystem) extends LibKryoSerializer[ActorRef] {

  override def write(kryo: Kryo, out: Output, ref: ActorRef): Unit =
    out.writeString(Serialization.serializedActorPath(ref))

  override def read(kryo: Kryo, in: Input, typ: Class[ActorRef]): ActorRef =
    system.provider.resolveActorRef(in.readString())

}
