package tw;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")

public class TwTest {
	public static class WordWithCount {

		public String word;
		public long count;

		public WordWithCount() {}

		public WordWithCount(String word, long count) {
			this.word = word;
			this.count = count;
		}

		@Override
		public String toString() {
			return word + " : " + count;
		}
	}
	
	public static void main(String[] args) throws Exception { 
		final String hostname;
		final int port;
		try {
			final ParameterTool params = ParameterTool.fromArgs(args);
			hostname = params.has("hostname") ? params.get("hostname") : "localhost";
			port = params.getInt("port");
		} catch (Exception e) {
			System.err.println("No port specified. Please run 'SocketWindowWordCount " +
				"--hostname <hostname> --port <port>', where hostname (localhost by default) " +
				"and port is the address of the text server");
			System.err.println("To start a simple text server, run 'netcat -l <port>' and " +
				"type the input text into the command line");
			return;
		}

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<String> text = env.socketTextStream(hostname, port, "\n");
		DataStream<WordWithCount> windowCounts = text
				.flatMap(new FlatMapFunction<String, WordWithCount>() {
					public void flatMap(String value, Collector<WordWithCount> out) {
						for (String word : value.split("\\s")) { // split by space per sentence -> array of words
							out.collect(new WordWithCount(word, 1L));
						}
					}
					// flatten array of sentences(array of words) (2d) to array of wordwithcount in (word, count) (1d)
				})

				.keyBy("word") // word in wordwithcount
				.timeWindow(Time.seconds(5)) // reduce all wordwithcount in this window,  5 seconds a window

				.reduce(new ReduceFunction<WordWithCount>() {
					public WordWithCount reduce(WordWithCount a, WordWithCount b) {
						return new WordWithCount(a.word, a.count + b.count);
					}
				});
		windowCounts.print().setParallelism(1); // only one thread
		env.execute("Window WordCount");

	}

}
