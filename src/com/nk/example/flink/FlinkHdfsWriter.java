package com.nk.example.flink;

import java.util.Properties;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.DateTimeBucketer;
import org.apache.flink.streaming.connectors.fs.RollingSink;
import org.apache.flink.streaming.connectors.fs.SequenceFileWriter;
import org.apache.flink.util.Collector;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class FlinkHdfsWriter {
	public static void main(String[] args) {

		DataStream<Tuple2<IntWritable, Text>> input = getHdfsStream(
				FlinkKafkaSimpleConsumer.getFlinkKafkaSimpleConsumerStream());
		
		input.addSink(getHdfsSink());
		
	}

	public static RollingSink getHdfsSink() {
		RollingSink sink = new RollingSink<String>("/base/path");
		sink.setBucketer(new DateTimeBucketer("yyyy-MM-dd--HHmm"));
		sink.setWriter(new SequenceFileWriter<IntWritable, Text>());
		sink.setBatchSize(1024 * 1024 * 400); // this is 400 MB,
		return sink;
	}

	private static DataStream<Tuple2<IntWritable, Text>> getHdfsStream(DataStream<String> stream) {
		return stream.flatMap(new HdfsLinkSplitter());
	}


	public static class HdfsLinkSplitter implements FlatMapFunction<String, Tuple2<IntWritable, Text>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(String line, Collector<Tuple2<IntWritable, Text>> out) {
			for (String word : line.split(" ")) {
				out.collect(new Tuple2<IntWritable, Text>(new IntWritable(1), new Text(word)));
			}
		}
	}

}
