package com.github.lucastorri.moca.store.serialization

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import akka.actor.{ActorSystem, ExtendedActorSystem, ActorRef}
import akka.persistence.PersistentRepr
import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer => KryoSerializer}
import org.objenesis.strategy.StdInstantiatorStrategy

import scala.reflect.ClassTag

class KryoSerialization[T](system: ActorSystem)(implicit tag: ClassTag[T]) {

  private val extendedSystem = system.asInstanceOf[ExtendedActorSystem]

  private val extraSerializers = Map[Class[_], KryoSerializer[_]](
    classOf[ActorRef] -> ActorRefKryoSerializer(extendedSystem)
  )

  def kryo: Kryo = {
    val k = new Kryo()
    k.setRegistrationRequired(false)
    k.setInstantiatorStrategy(new StdInstantiatorStrategy)
    extraSerializers.foreach { case (clazz, serializer) => k.addDefaultSerializer(clazz, serializer) }
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
