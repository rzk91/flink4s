package com.rzk91.flink4s

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.{
  CheckpointConfig,
  StreamExecutionEnvironment => JavaEnv
}
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.graph.StreamGraph

import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._

final case class StreamExecutionEnvironment(javaEnv: JavaEnv) {

  def getStreamGraph: StreamGraph = javaEnv.getStreamGraph

  def setParallelism(n: Int): StreamExecutionEnvironment = {
    javaEnv.setParallelism(n)
    StreamExecutionEnvironment(javaEnv)
  }

  def setStateBackend(backend: StateBackend): StreamExecutionEnvironment = {
    javaEnv.setStateBackend(backend)
    StreamExecutionEnvironment(javaEnv)
  }

  def fromCollection[T](data: Seq[T])(implicit typeInfo: TypeInformation[T]): DataStream[T] =
    DataStream(javaEnv.fromCollection(data.asJava, typeInfo))

  def addSource[T](function: SourceFunction[T])(implicit
      typeInfo: TypeInformation[T]
  ): DataStream[T] =
    DataStream(javaEnv.addSource(function, typeInfo))

  def setRestartStrategy(
      restartStrategy: RestartStrategies.RestartStrategyConfiguration
  ): StreamExecutionEnvironment = {
    javaEnv.setRestartStrategy(restartStrategy)
    StreamExecutionEnvironment(javaEnv)
  }

  def enableCheckpointing(interval: Duration, mode: CheckpointingMode): StreamExecutionEnvironment =
    StreamExecutionEnvironment(javaEnv.enableCheckpointing(interval.toMillis, mode))

  def execute: JobExecutionResult = javaEnv.execute()

  def getCheckpointConfig: CheckpointConfig = javaEnv.getCheckpointConfig

}

object StreamExecutionEnvironment {
  def getExecutionEnvironment: StreamExecutionEnvironment =
    StreamExecutionEnvironment(JavaEnv.getExecutionEnvironment())
}
