package com.github.lucastorri.moca.config

import java.lang.reflect.Constructor

import com.typesafe.config.Config

case class ClassBuilder[T](className: String, params: (Class[_], AnyRef)*) {
  
  private val availableParams = params.toMap

  private val clazz = Class.forName(className)

  def build(): Option[T] =
    clazz.getConstructors.find(canBuild).map(c => c.newInstance(paramsFor(c): _*).asInstanceOf[T])

  def apply(): T =
    build().getOrElse(sys.error(s"Parameters not available for class $className"))

  def paramsFor(c: Constructor[_]): Array[AnyRef] =
    c.getParameterTypes.map(availableParams)

  def canBuild(c: Constructor[_]): Boolean =
    c.getParameterTypes.forall(param => availableParams.contains(param))

}

object ClassBuilder {

  def fromConfig[T](cfg: Config, params: (Class[_], AnyRef)*): ClassBuilder[T] =
    ClassBuilder[T](cfg.getString("class"), params :+ (classOf[Config] -> cfg): _*)

}
