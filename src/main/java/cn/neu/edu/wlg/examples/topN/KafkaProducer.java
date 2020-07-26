package cn.neu.edu.wlg.examples.topN;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class KafkaProducer {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> text = env.addSource(new MyNoParalleSource()).setParallelism(1);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.server", "127.0.0.1:9092");
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<String>("topn", new SimpleStringSchema(), properties);
        // event-timestamp 事件的发生时间
        producer.setWriteTimestampToKafka(true);
        env.execute();
    }
}
