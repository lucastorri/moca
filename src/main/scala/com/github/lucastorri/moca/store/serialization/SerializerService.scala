package com.github.lucastorri.moca.store.serialization

import scala.reflect.ClassTag

trait SerializerService {

  def create[T : ClassTag]: Serializer[T]

}
