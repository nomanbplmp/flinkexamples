package com.nk.example.flink;

import java.util.Properties;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
/**
 * 
 * @author noman
 *
 */



public class FlinkKafkaProducer {
	public static void main(String[] args) {

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("zookeeper.connect", "localhost:2181");
		properties.setProperty("group.id", "test");
		configureKafkaProducer(properties);

	}

	public static void configureKafkaProducer(Properties properties) {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(5000); // checkpoint every 5000 msecs

		FlinkKafkaProducer08<String> producer = new FlinkKafkaProducer08<>("localhost:9092", "my-topic",
				new SimpleStringSchema());
		
		// topic to write on
		FlinkKafkaConsumer08<String> consumer = new FlinkKafkaConsumer08<>("topic", new SimpleStringSchema(),
				properties);

		DataStream<String> stream = env.addSource(consumer);

		stream.addSink(producer);
	}
}
