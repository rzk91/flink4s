package com.rzk91.flink4s

import org.apache.flink.api.common.functions.AbstractRichFunction
import org.apache.flink.api.common.state._
import org.apache.flink.api.common.typeinfo.TypeInformation

import scala.jdk.CollectionConverters._

trait FlinkStateHelper { _: AbstractRichFunction =>

  def valueState[A: TypeInformation](name: String): ValueState[A] =
    getRuntimeContext.getState(new ValueStateDescriptor[A](name, implicitly[TypeInformation[A]]))

  def mapState[K: TypeInformation, V: TypeInformation](name: String): MapState[K, V] =
    getRuntimeContext.getMapState(
      new MapStateDescriptor[K, V](
        name,
        implicitly[TypeInformation[K]],
        implicitly[TypeInformation[V]]
      )
    )

  def listState[A: TypeInformation](name: String): ListState[A] =
    getRuntimeContext.getListState(new ListStateDescriptor[A](name, implicitly[TypeInformation[A]]))
}

object FlinkStateHelper {

  implicit class ValueStateOps[A](private val vs: ValueState[A]) extends AnyVal {
    def valueOption: Option[A]        = Option(vs.value)
    def valueOrElse(default: => A): A = valueOption.getOrElse(default)

    // Further utility methods (shortcut to equivalent `Option` methods)
    def isDefined: Boolean               = valueOption.isDefined
    def isEmpty: Boolean                 = valueOption.isEmpty
    def exists(f: A => Boolean): Boolean = valueOption.exists(f)
    def forall(f: A => Boolean): Boolean = valueOption.forall(f)
    def contains(a: A): Boolean          = valueOption.contains(a)
  }

  implicit class MapStateOps[K, V](private val ms: MapState[K, V]) extends AnyVal {

    def getOption(key: K): Option[V]                          = Option(ms.get(key))
    def nonEmpty: Boolean                                     = !ms.isEmpty
    def getOrElse(key: K, default: => V): V                   = getOption(key).getOrElse(default)
    def getWith[W](key: K)(f: V => W): Option[W]              = getOption(key).map(f)
    def getOrElseWith[W](key: K, default: => W)(f: V => W): W = getOption(key).fold(default)(f)

    def asIterable: Iterable[(K, V)]      = ms.entries.asScala.map(kv => (kv.getKey, kv.getValue))
    def asIterableOnlyKeys: Iterable[K]   = ms.keys.asScala
    def asIterableOnlyValues: Iterable[V] = ms.values.asScala

    def asIterableWith[K1, V1](fk: K => K1)(fv: V => V1): Iterable[(K1, V1)] =
      ms.entries.asScala.map(kv => (fk(kv.getKey), fv(kv.getValue)))

    def asIterableMapValues[V1](f: V => V1): Iterable[(K, V1)] = asIterableWith(identity)(f)

    def asIterableTransform[O](f: (K, V) => O): Iterable[O] = asIterable.map { case (k, v) =>
      f(k, v)
    }
    def asScalaMap: Map[K, V] = asIterable.toMap

    def filter(p: (K, V) => Boolean): Iterable[(K, V)] = asIterable.filter { case (k, v) =>
      p(k, v)
    }

    def update(map: Map[K, V]): Unit = ms.putAll(map.asJava)
  }

  implicit class ListStateOps[A](private val ls: ListState[A]) extends AnyVal {
    def getOption: Option[List[A]]              = Option(ls.get).map(_.asScala.toList)
    def getOrElse(default: => List[A]): List[A] = getOption.getOrElse(default)
    def addIfNotPresent(a: A): Unit             = if (!contains(a)) ls.add(a)
    def length: Int                             = getOption.fold(0)(_.length)
    def size: Int                               = length
    def exists(f: A => Boolean): Boolean        = getOption.exists(_.exists(f))
    def contains(a: A): Boolean                 = exists(_ == a)
    def getListUnsafe: List[A]                  = ls.get.asScala.toList
    def isEmpty: Boolean                        = getOption.fold(true)(_.lengthIs == 0)
    def isDefined: Boolean                      = getOption.isDefined
    def nonEmpty: Boolean                       = !isEmpty
    def headOption: Option[A]                   = getOption.flatMap(_.headOption)
    def lastOption: Option[A]                   = getOption.flatMap(_.lastOption)
    def updateList(list: List[A]): Unit         = ls.update(list.asJava)
    def appendAll(list: List[A]): Unit          = ls.addAll(list.asJava)
    def appendAll(as: A*): Unit                 = ls.addAll(as.asJava)
    def removeAll(a: A): Unit              = getOption.foreach(l => updateList(l.filterNot(_ == a)))
    def zipWithIndexUnsafe: List[(A, Int)] = getListUnsafe.zipWithIndex
    def zipWithIndex: Option[List[(A, Int)]] = getOption.map(_.zipWithIndex)
  }
}
