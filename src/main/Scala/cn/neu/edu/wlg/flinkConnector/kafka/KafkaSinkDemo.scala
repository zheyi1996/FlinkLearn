package cn.neu.edu.wlg.flinkConnector.kafka

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.api.scala._

object KafkaSinkDemo {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val inputStream = env.readTextFile("hdfs://master:9000/wlg/data/temperatureSensor.txt");
    val dataStream: DataStream[String] = inputStream
      .map(data => {
        val dataArray = data.split(" ")
        (dataArray(0), dataArray(1).toLong, dataArray(2).toDouble).toString
      })
    dataStream.addSink(new FlinkKafkaProducer[String]("master:9092", "test", new SimpleStringSchema()))
    env.execute("KafkaSinkDemo")
  }
}
