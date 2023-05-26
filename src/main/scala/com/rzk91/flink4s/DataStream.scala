package com.rzk91.flink4s

import org.apache.flink.api.common.functions.{FilterFunction, FlatMapFunction, MapFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.api.java.typeutils.ResultTypeQueryable
import org.apache.flink.streaming.api.datastream.{
  AsyncDataStream,
  DataStreamSink,
  SingleOutputStreamOperator,
  DataStream => JavaStream,
  KeyedStream => JavaKeyedStream
}
import org.apache.flink.streaming.api.functions.async.{ResultFuture, RichAsyncFunction}
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.util.Collector

import java.util.Collections
import java.util.concurrent.TimeUnit
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.jdk.FutureConverters._

final case class DataStream[T](stream: JavaStream[T])(implicit typeInfo: TypeInformation[T]) {

  def map[R](f: T => R)(implicit typeInfo: TypeInformation[R]): DataStream[R] = {
    val mapper = new MapFunction[T, R] with ResultTypeQueryable[R] {
      def map(in: T): R                                = f(in)
      override def getProducedType: TypeInformation[R] = typeInfo
    }
    DataStream(stream.map(mapper, typeInfo))
  }

  def flatMap[R](f: T => IterableOnce[R])(implicit typeInfo: TypeInformation[R]): DataStream[R] = {
    val flatMapper = new FlatMapFunction[T, R] with ResultTypeQueryable[R] {
      def flatMap(in: T, out: Collector[R]): Unit      = f(in).iterator.foreach(out.collect)
      override def getProducedType: TypeInformation[R] = typeInfo
    }
    DataStream(stream.flatMap(flatMapper))
  }

  def orderedAsync[R](
      fun: RichAsyncFunction[T, R],
      timeout: Duration = 10.seconds,
      capacity: Int = 20
  )(implicit typeInfo: TypeInformation[R]): DataStream[R] = {
    val out = AsyncDataStream.orderedWait(
      stream,
      fun,
      timeout.toMillis,
      TimeUnit.MILLISECONDS
    )
    DataStream(out)
  }

  def orderedMapAsync[R](
      f: T => Future[R],
      timeout: Duration = 10.seconds,
      capacity: Int = 20
  )(implicit typeInfo: TypeInformation[R]): DataStream[R] =
    orderedAsync(DataStream.asyncFunctionFromFuture(f), timeout, capacity)

  def unorderedAsync[R](
      fun: RichAsyncFunction[T, R],
      timeout: Duration = 10.seconds,
      capacity: Int = 20
  )(implicit typeInfo: TypeInformation[R]): DataStream[R] = {
    val out = AsyncDataStream.unorderedWait(
      stream,
      fun,
      timeout.toMillis,
      TimeUnit.MILLISECONDS
    )
    DataStream(out)
  }

  def unorderedMapAsync[R](
      f: T => Future[R],
      timeout: Duration = 10.seconds,
      capacity: Int = 20
  )(implicit typeInfo: TypeInformation[R]): DataStream[R] =
    unorderedAsync(DataStream.asyncFunctionFromFuture(f), timeout, capacity)

  def filter(f: T => Boolean): DataStream[T] = {
    val filter = new FilterFunction[T] {
      def filter(t: T): Boolean = f(t)
    }
    DataStream(stream.filter(filter))
  }

  def filterNot(f: T => Boolean): DataStream[T] = filter(!f(_))

  def collect[R](pf: PartialFunction[T, R])(implicit typeInfo: TypeInformation[R]): DataStream[R] =
    filter(pf.isDefinedAt).map(pf)

  def keyBy[K](f: T => K)(implicit keyTypeInfo: TypeInformation[K]): KeyedStream[T, K] = {
    val keyExtractor = new KeySelector[T, K] with ResultTypeQueryable[K] {
      def getKey(in: T): K                             = f(in)
      override def getProducedType: TypeInformation[K] = keyTypeInfo
    }
    KeyedStream(new JavaKeyedStream(stream, keyExtractor, keyTypeInfo))
  }

  def union(dataStreams: DataStream[T]*): DataStream[T] =
    DataStream(stream.union(dataStreams.map(_.stream): _*))

  def addSink(sinkFunction: SinkFunction[T]): DataStreamSink[T] =
    stream.addSink(sinkFunction)

  def runAndCollect: List[T] = stream.executeAndCollect().asScala.toList

  def print(): DataStreamSink[T] = stream.print()

}

object DataStream {
  def apply[T: TypeInformation](stream: SingleOutputStreamOperator[T]): DataStream[T] =
    new DataStream[T](stream.asInstanceOf[JavaStream[T]])

  def asyncFunctionFromFuture[T, R](f: T => Future[R])(implicit
      typeInfo: TypeInformation[R]
  ): RichAsyncFunction[T, R] =
    new RichAsyncFunction[T, R] with ResultTypeQueryable[R] {
      override def getProducedType: TypeInformation[R] = typeInfo

      override def asyncInvoke(in: T, result: ResultFuture[R]): Unit =
        f(in).asJava.whenComplete { (response, error) =>
          Option(response) match {
            case Some(r) => result.complete(Collections.singletonList(r))
            case None    => result.completeExceptionally(error)
          }
        }
    }

}
