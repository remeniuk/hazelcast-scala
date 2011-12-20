package org.scalaby.hazelcastwf

import scalaz._
import Scalaz._

/**
 * User: remeniuk
 */

object HazelcastScalaz extends Applys {

  implicit def DistributedTaskFunctor: Functor[DistributedTask] = new Functor[DistributedTask] with Serializable {
    def fmap[A, B](r: DistributedTask[A], f: A => B) = r map f
  }

  implicit def DistributedTaskBind: Bind[DistributedTask] = new Bind[DistributedTask] with Serializable {
    def bind[A, B](a: DistributedTask[A], f: (A) => DistributedTask[B]): DistributedTask[B] =
      a.flatMap(f)
  }

  def SerializableFunctorBindApply[Z[_]](implicit t: Functor[Z], b: Bind[Z]) = new Apply[Z] with Serializable {
    def apply[A, B](f: Z[A => B], a: Z[A]): Z[B] = {
      lazy val fv = f
      lazy val fa = a
      b.bind(fv, (g: A => B) => t.fmap(fa, g(_: A)))
    }
  }

  implicit val DistributedTaskApply = SerializableFunctorBindApply[DistributedTask]

}