package org.scalaby.hazelcastwf


/**
 * User: remeniuk
 */

object Primitives {

  private val primitivesMapping: Map[Class[_], Class[_]] =
    Map(
      classOf[Int] -> classOf[java.lang.Integer],
      classOf[Long] -> classOf[java.lang.Long],
      classOf[Double] -> classOf[java.lang.Double],
      classOf[Float] -> classOf[java.lang.Float],
      classOf[Byte] -> classOf[java.lang.Byte],
      classOf[Boolean] -> classOf[java.lang.Boolean],
      classOf[Short] -> classOf[java.lang.Short],
      classOf[Symbol] -> classOf[java.lang.String]
    )

  def box(clazz: Class[_]) = primitivesMapping.get(clazz).getOrElse(clazz)

}