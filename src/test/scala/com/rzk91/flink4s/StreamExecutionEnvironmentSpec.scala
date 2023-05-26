package com.rzk91.flink4s

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.CheckpointingMode
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

final class StreamExecutionEnvironmentSpec extends AnyFunSpec with Matchers {

  describe("StreamExecutionEnvironment") {
    it("should have an option to enable checkpointing") {
      val env = FlinkExecutor.newEnv(parallelism = 1)
      env.enableCheckpointing(10.seconds, CheckpointingMode.AT_LEAST_ONCE)
      val cm = env.getCheckpointConfig.getCheckpointingMode
      cm should equal(CheckpointingMode.AT_LEAST_ONCE)
    }

    it("should have an option to set restart strategies") {
      val env = FlinkExecutor.newEnv(parallelism = 1)
      env.setRestartStrategy(RestartStrategies.noRestart)
      env.javaEnv.getRestartStrategy should equal(RestartStrategies.noRestart)
    }
  }

}
