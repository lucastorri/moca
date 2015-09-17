package com.github.lucastorri.moca.store.serialization

trait Serializer[T] {

  def serialize(t: T): Array[Byte]

  def deserialize(buf: Array[Byte]): T

}
