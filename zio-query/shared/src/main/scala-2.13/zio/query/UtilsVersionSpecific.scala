package zio.query

import scala.collection.mutable

private[query] object UtilsVersionSpecific {
  private final val DefaultLoadFactor = 0.75d

  def newHashMap[K, V](expectedNumElements: Int): mutable.HashMap[K, V] =
    new mutable.HashMap[K, V](sizeFor(expectedNumElements, DefaultLoadFactor), DefaultLoadFactor)

  private def sizeFor(nElements: Int, loadFactor: Double): Int =
    ((nElements + 1).toDouble / loadFactor).toInt
}
