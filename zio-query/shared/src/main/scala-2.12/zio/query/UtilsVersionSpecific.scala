package zio.query

import zio.Chunk

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

private[query] object UtilsVersionSpecific {

  def newHashMap[K, V](expectedNumElements: Int): mutable.HashMap[K, V] = {
    val map = mutable.HashMap.empty[K, V]
    map.sizeHint(expectedNumElements)
    map
  }

  // Methods that don't exist in Scala 2.12 so we add them as syntax

  implicit class LiftCoSyntax[E, A, B](private val ev: A <:< Request[E, B]) extends AnyVal {
    def liftCo(in: Chunk[A]): Chunk[Request[E, B]] = in.asInstanceOf[Chunk[Request[E, B]]]
  }

  implicit class HashMapSyntax[K, V](private val map: collection.mutable.HashMap[K, V]) extends AnyVal {
    def addAll(elems: Iterable[(K, V)]): collection.mutable.HashMap[K, V] = map ++= elems
  }

}
