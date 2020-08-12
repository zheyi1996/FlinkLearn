package cn.neu.edu.wlg.flinkConnector.kafka

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.streaming.api.scala._

object KafkaSourceSinkDemo {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "master:9092")
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    println("----------test---------")
    val inputStream = env.addSource(new FlinkKafkaConsumer[String]("test", new SimpleStringSchema, properties))
    inputStream.addSink(new FlinkKafkaProducer[String]("master:9092", "test1", new SimpleStringSchema()))
    env.execute("KafkaSinkDemo")
  }
}
