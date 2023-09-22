package org.demo.logs;

/**
 * Simple model for the relevant fields in a json log message with the following structure:
 * 
 *    {
        "ec2":
        {
            "sn": "tap-delivery--prod-east--v2438--2023-01-10-01-34-utc--o8rvapsp6a4dq8qj",
            "hn": "ip-10-202-149-109",
            "ip": "10.202.149.109",
            "id": "i-02bd018df9ea54f91",
            "az": "us-east-1b"
        },
        "env": "prod-east",
        "host": "ip-10-202-149-109",
        "m":
        {
            "sv": "v2438",
            "si": "tap-delivery",
            "di": "o8rvapsp6a4dq8qj",
            "g": "WebServer",
            "t": "application"
        },
        "micros_container": "platform-slauth",
        "policyControl":
        {
            "decision_id": "c1cb241c-bdab-4937-a3d0-80620edf68af",
            "input":
            {
                "headers":
                {
                   "<all_headers>"
                },
                "mechanism": "asap",
                "method": "POST",
                "path": "/random/path",
                "principals":
                [
                    "micros-group/jira"
                ],
                "remote_addr": "10.202.144.143"
            },
            "labels":
            {
                "platform": "9010df6581faafbd410a13592f9e6c77e9b2771ce11094fb3249adbfeb9c9c97",
                "service": "98c93903b1dd4a3693970661abee00605b7c2a1e568de0d57849308a17dca8f8"
            },
            "metrics":
            {
                "rego_query_eval": 500740
            },
            "query": "data.platform.decision",
            "result":
            {
                "authorize": true
            },
            "timestamp": "2023-01-10T09:05:41.433934244Z"
        },
        "time": "2023-01-10T09:05:41.434079395Z",
        "@timestamp": "2023-01-10T09:05:41.434079395Z",
        "@laas":
        {
            "serviceId": "6d717e10-258e-4c94-ab73-a4c2c887df10",
            "source": "micros",
            "name": "tap-delivery",
            "organisation": "Messaging - Targeting",
            "owner": "jwyne",
            "environment": "prod-east",
            "seq": "496358574819307203350646888f66033704500781718211247497922",
            "subSeq": 88,
            "shard": "shardId-000000181804",
            "received": 1673341541538,
            "processed": 1673341541591,
            "delay": 53,
            "delta": 104,
            "size": 2184
        }
    } 
 */
public class LogEvent {
    private String si;
    private String env;
    private String msg;

    private LogEvent(String si, String env, String msg) {
        this.si = si;
        this.env = env;
        this.msg = msg;
    }

    public static LogEvent of(String si, String env, String msg) {
        return new LogEvent(si, env, msg);
    }

    public String getSi() {
        return si;
    }

    public String getEnv() {
        return env;
    }

    public String getMsg() {
        return msg;
    }

    @Override
    public String toString() {
        return "LogEvent [si=" + si + ", env=" + env + ", msg=" + msg + "]";
    }
}
