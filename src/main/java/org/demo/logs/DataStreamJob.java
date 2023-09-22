package org.demo.logs;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>
 * For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink
 * Website</a>.
 *
 * <p>
 * To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>
 * If you change the name of the main class (with the public static void
 * main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for
 * 'mainClass').
 */
public class DataStreamJob {

    public static void main(String[] args) throws Exception {
        // Sets up the execution environment, which is the main entry point
        // to building Flink applications.
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Add a mock source which generates LogEvents every 'n' milliseconds
        // A single stream of log events
        final DataStreamSource<LogEvent> source = env.addSource(new MockLogEventSource());

        // Mulitple streams where each stream contains log events just for that key
        final KeyedStream<LogEvent, String> keyedStream = source.keyBy(event -> event.getEnv() + ":" + event.getSi());

        // Mulitple streams where each stream contains a collection of log events just
        // for that key
        // that arrived in a 5 second window.
        final WindowedStream<LogEvent, String, TimeWindow> windowedStream = keyedStream
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        // This is where we apply logic for processing our now partitioned and windowed
        // set of events.
        // S3LogWriter is an implementation of the AggregateFunction interface that
        // operates on individual
        // events as they arrive in the window.
        // ClosLogWindowFunction is an implementation of the ProcessWindowFunction
        // interface that
        // runs when the window closes and can process the final output of the
        // aggregator.
        windowedStream.aggregate(new S3LogWriter(), new CloseLogWindowFunction());

        // The above is equivalent to the following chained operations. This would be
        // how i would write my production
        // code, I just broke it up into intermediate steps above to make it clearer how
        // the stream
        // topology is getting built up. Once you understand how Flink is working this
        // is much more
        // concise and easy to read.
        //
        // env.addSource(new MockLogEventSource())
        //     .keyBy(event -> event.getEnv() + ":" + event.getSi())
        //     .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
        //     .aggregate(new S3LogWriter(), new CloseLogWindowFunction());

        // Execute program, beginning computation.
        env.execute("Log Stream Processor");
    }

    /**
     * The canonical use for an AggregateFunction is to use it for computing running
     * totals, averages etc.
     * We have repurposed it here to work as a buffered writer, where it accumulate
     * events up to a max
     * buffer size before writing them out. Finally it will write any remaining
     * items in the buffer at the
     * point of the window closing.
     */
    private static class S3LogWriter
            implements AggregateFunction<LogEvent, Tuple2<List<LogEvent>, Long>, Long> {

        // Number of records to accumulate before writing
        private static final long WRITE_SIZE = 10;

        @Override
        public Tuple2<List<LogEvent>, Long> createAccumulator() {
            return new Tuple2<>(new ArrayList<>(), 0L);
        }

        @Override
        public Tuple2<List<LogEvent>, Long> add(LogEvent value, Tuple2<List<LogEvent>, Long> accumulator) {
            // Add event to current list
            List<LogEvent> events = accumulator.f0;
            events.add(value);

            // If list size is at or great in size then our preferred write size
            // write out current events and clear the list
            if (events.size() >= WRITE_SIZE) {
                System.out.println("Buffer write(" + events.get(0).getEnv() + ":" + events.get(0).getSi() + ")");
                events.clear();
            }
            ;

            return new Tuple2<>(events, ++accumulator.f1);
        }

        @Override
        public Long getResult(Tuple2<List<LogEvent>, Long> accumulator) {
            // If we have remaining events in the accumulator then write them out
            // The getResult function will be triggered by the window close function
            if (accumulator.f0.size() > 0) {
                System.out.println(
                        "Buffer write(" + accumulator.f0.get(0).getEnv() + ":" + accumulator.f0.get(0).getSi() + ")");
            }

            // Return the number of events processed.
            return accumulator.f1;
        }

        @Override
        public Tuple2<List<LogEvent>, Long> merge(Tuple2<List<LogEvent>, Long> a, Tuple2<List<LogEvent>, Long> b) {
            a.f0.addAll(b.f0);
            return new Tuple2<>(a.f0, a.f1 + b.f1);
        }
    }

    private static class CloseLogWindowFunction
            extends ProcessWindowFunction<Long, Tuple2<String, Long>, String, TimeWindow> {

        public void process(String key,
                Context context,
                Iterable<Long> processedRecords,
                Collector<Tuple2<String, Long>> out) {
            Long count = processedRecords.iterator().next();
            System.out.println("Window close(" + key + "): " + count + " records");
            out.collect(new Tuple2<>(key, count));
        }
    }
}
