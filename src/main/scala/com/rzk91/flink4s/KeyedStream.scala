package com.rzk91.flink4s

import cats.Semigroup
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.datastream.{KeyedStream => JavaKeyedStream}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow

final case class KeyedStream[T, K](stream: JavaKeyedStream[T, K])(implicit
    typeInfo: TypeInformation[T],
    keyInfo: TypeInformation[K]
) {

  def reduce(f: (T, T) => T): DataStream[T] = {
    val reducer = new ReduceFunction[T] {
      def reduce(v1: T, v2: T): T = f(v1, v2)
    }
    DataStream(stream.reduce(reducer))
  }

  def combine(implicit semi: Semigroup[T]): DataStream[T] = reduce(semi.combine)

  def connect[T2, K2](otherKeyedStream: KeyedStream[T2, K2])(implicit
      tTypeInfo: TypeInformation[T2],
      kTypeInfo: TypeInformation[K2]
  ): ConnectedStreams[T, T2] =
    ConnectedStreams(stream.connect(otherKeyedStream.stream))

  def countWindow(size: Long): WindowedStream[T, K, GlobalWindow] =
    WindowedStream(stream.countWindow(size))

  def countWindow(size: Long, slide: Long): WindowedStream[T, K, GlobalWindow] =
    WindowedStream(stream.countWindow(size, slide))

}
