package zio.query

import scala.collection.mutable

private[query] object CollectionUtilsVersionSpecific {

  def newHashMap[K, V](expectedNumElements: Int): mutable.HashMap[K, V] = {
    val map = mutable.HashMap.empty[K, V]
    map.sizeHint(expectedNumElements)
    map
  }

}
