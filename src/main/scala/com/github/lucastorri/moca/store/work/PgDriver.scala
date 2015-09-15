package com.github.lucastorri.moca.store.work

import com.github.tminglei.slickpg.PgArraySupport
import slick.driver.PostgresDriver
import slick.jdbc.{GetResult, PositionedParameters, PositionedResult, SetParameter}
import com.github.tminglei.slickpg.utils.SimpleArrayUtils._

object PgDriver extends PostgresDriver with PgArraySupport {

  override val api = new API with ArrayImplicits with MyArrayImplicitsPlus {}

  trait MyArrayImplicitsPlus extends SimpleArrayPlainImplicits {

    implicit val advancedStringListTypeMapper = new AdvancedArrayJdbcType[String]("text",
      fromString(identity)(_).orNull, mkString(identity))

    implicit object SetByteArray extends SetParameter[Array[Byte]] {
      def apply(v: Array[Byte], pp: PositionedParameters) {
        pp.setBytes(v)
      }
    }

    implicit object SetByteArrayOption extends SetParameter[Option[Array[Byte]]] {
      def apply(v: Option[Array[Byte]], pp: PositionedParameters) {
        pp.setBytesOption(v)
      }
    }

    implicit object GetByteArray extends GetResult[Array[Byte]] {
      def apply(rs: PositionedResult) = rs.nextBytes()
    }

    implicit object GetByteArrayOption extends GetResult[Option[Array[Byte]]] {
      def apply(rs: PositionedResult) = rs.nextBytesOption()
    }

    implicit object GetStringArray extends GetResult[Seq[String]] {
      override def apply(rs: PositionedResult): Seq[String] = rs.nextArray[String]()
    }

    implicit object GetStringArrayOption extends GetResult[Option[Seq[String]]] {
      override def apply(rs: PositionedResult): Option[Seq[String]] = rs.nextArrayOption()
    }

  }

}