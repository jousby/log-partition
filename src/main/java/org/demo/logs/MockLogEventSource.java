package org.demo.logs;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class MockLogEventSource implements SourceFunction<LogEvent> {
    private volatile boolean isRunning = true;
    private Random rand = new Random();

    private List<String> siList = Arrays.asList("monarch", "kamino", "manifold");
    private List<String> envList = Arrays.asList("prod-east", "prod-west", "dev-east", "dev-west");
    private List<String> msgList = Arrays.asList("logmesg1", "logmesg2", "logmesg3", "logmesg4");

    public void run(SourceContext<LogEvent> ctx) throws Exception {
        while (isRunning) {
            ctx.collect(createMockLogEvent());
            Thread.sleep(10);
        }
    }

    public void cancel() {
        isRunning = false;
    }

    public LogEvent createMockLogEvent() {
        // The random service
        int randomIndex = rand.nextInt(siList.size());
        String si = siList.get(randomIndex);

        // The random env
        randomIndex = rand.nextInt(envList.size());
        String env = envList.get(randomIndex);

        // The random log message
        randomIndex = rand.nextInt(msgList.size());
        String msg = msgList.get(randomIndex);

        return LogEvent.of(si, env, msg);
    }
}
