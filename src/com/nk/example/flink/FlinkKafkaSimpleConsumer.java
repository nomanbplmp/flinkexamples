package com.nk.example.flink;

import java.util.Properties;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

/**
 * 
 * 
 * 
 * @author noman
 *
 *
 *
 *         Flink’s Kafka consumer is called FlinkKafkaConsumer08 (or 09 for
 *         Kafka 0.9.0.x versions). It provides access to one or more Kafka
 *         topics.
 * 
 *         The constructor accepts the following arguments:
 * 
 *         The topic name / list of topic names A DeserializationSchema /
 *         KeyedDeserializationSchema for deserializing the data from Kafka
 *         Properties for the Kafka consumer. The following properties are
 *         required: “bootstrap.servers” (comma separated list of Kafka brokers)
 *         “zookeeper.connect” (comma separated list of Zookeeper servers) (only
 *         required for Kafka 0.8) “group.id” the id of the consumer group
 *
 *
 * 
 */



public class FlinkKafkaSimpleConsumer {
public static void main(String[] args) {

DataStream<String> stream = getFlinkKafkaSimpleConsumerStream();
stream.addSink(getConsoleSink());

}

public static SinkFunction<String> getConsoleSink() {
	return new SinkFunction<String>() {
	
		private static final long serialVersionUID = 1L;
	
		@Override
		public void invoke(String value) throws Exception {
			System.out.println(value);
			
		}
	};
}

public static DataStream<String> getFlinkKafkaSimpleConsumerStream() {
	final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
	
	env.enableCheckpointing(5000); // checkpoint every 5000 msecs
	
	Properties properties = new Properties();
	properties.setProperty("bootstrap.servers", "localhost:9092");
	
	properties.setProperty("zookeeper.connect", "localhost:2181");
	properties.setProperty("group.id", "test");
	DataStream<String> stream = env
		.addSource(new FlinkKafkaConsumer08<>("topic", new SimpleStringSchema(), properties));
	return stream;
}
}
