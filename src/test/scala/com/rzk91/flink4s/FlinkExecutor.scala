package com.rzk91.flink4s

import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.test.util.MiniClusterWithClientResource

object FlinkExecutor {

  private var flinkCluster: MiniClusterWithClientResource = _

  def startCluster(): Unit = {
    flinkCluster = new MiniClusterWithClientResource(
      new MiniClusterResourceConfiguration.Builder()
        .setNumberSlotsPerTaskManager(2)
        .setNumberTaskManagers(2)
        .build
    )
    flinkCluster.before()
  }

  def stopCluster(): Unit = flinkCluster.after()

  def newEnv(parallelism: Int = 2): StreamExecutionEnvironment = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(parallelism)
    val rocks = new EmbeddedRocksDBStateBackend()
    env.setStateBackend(rocks)
    env
  }

}
