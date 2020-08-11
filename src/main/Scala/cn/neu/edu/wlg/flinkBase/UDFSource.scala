package cn.neu.edu.wlg.flinkBase

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.util.Random
import org.apache.flink.streaming.api.scala._

object UDFSource {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.addSource(new MySensorSource())

    stream.print("stream")
    env.execute("UDFSourceFunctionTest")
  }
}
// 实现一个自定义的SourceFunction 自动生成测试数据
class MySensorSource() extends SourceFunction[SensorReading] {
  var running: Boolean = true
  // 随机生成SensorReading数据
  override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
    // 定义随机数发生器
    val rand = new Random()
    // 随机生成10个传感器的温度值，并且不停在之前温度基础上更新
    // 首先生成十个传感器的初始温度
    var curTemps = 1.to(10).map(
      i => ("sensor_" + i, 30 + rand.nextGaussian() * 10)
    )
    // 无限循环生成随机数据
    while (running) {
      // 在当前温度基础上随机生成微小波动
      curTemps = curTemps.map(
        data => (data._1, data._2 + rand.nextGaussian())
      )
      // 获取当前系统时间
      var curTime = System.currentTimeMillis()
      // 包装成样例类，用ctx发出数据
      curTemps.foreach(
        data => ctx.collect(SensorReading(data._1, curTime, data._2))
      )
      // 定义间隔时间
      Thread.sleep(1000L)
    }
  }
  override def cancel(): Unit = running = false
}

case class SensorReading(id: String, timestamp: Long, temperature: Double)


