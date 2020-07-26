package cn.neu.edu.wlg.examples.topN;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.util.Comparator;
import java.util.Properties;
import java.util.TreeMap;
//import java.util.TreeMap;

public class TopN {

    public static void main(String[] args) throws Exception {
        // 每隔5s，计算过去十分钟的top3商品
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime); // 以processtime作为时间语义
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        FlinkKafkaConsumer<String> input = new FlinkKafkaConsumer<String>("topn", new SimpleStringSchema(), properties);
        // 从最早开始消费
        input.setStartFromEarliest();
        DataStream<String> stream = env.addSource(input);
        // 将输入语句split成一个一个单词并初始化count值为1的Tuple2<String, Integer>
        DataStream<Tuple2<String, Integer>> ds = stream.flatMap(new LineSplitter());
        DataStreamSink<String> wcount = ds
                .keyBy(0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(600), Time.seconds(5)))
                .sum(1)
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new TopNAllFunction(3))
                .print();
        env.execute();
    }
    private static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            out.collect(new Tuple2<String, Integer>(value, 1));
        }
    }
    private static final class TopNAllFunction extends ProcessAllWindowFunction<Tuple2<String, Integer>, String, TimeWindow> {
        private int topSize = 3;
        public TopNAllFunction(int topSize) {
            this.topSize = topSize;
        }

        @Override
        public void process(Context context, Iterable<Tuple2<String, Integer>> input, Collector<String> out) throws Exception {
            TreeMap<Integer, Tuple2<String, Integer>> treeMap = new TreeMap<>( // treeMap 安装key降序排列
                    new Comparator<Integer>() {
                        @Override
                        public int compare(Integer o1, Integer o2) {
                            return o1 > o2 ? 1 : -1;
                        }
                    }
            );
            for (Tuple2<String, Integer> element : input) {
                treeMap.put(element._2, element);
                if (treeMap.size() > topSize) { // 只保留TopN个元素
                    treeMap.pollLastEntry();
                }
            }
        }
    }
}
