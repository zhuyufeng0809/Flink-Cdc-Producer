package org.leqee.cdc.bean.message;

import java.math.BigInteger;
import java.util.Map;

public class DmlMessage {
    private Map<String, Object> before;
    private Map<String, Object> after;
    private Map<String, Object> source;
    private String op;
    private Long ts_ms;

    public DmlMessage() {
    }

    public DmlMessage(Map<String, Object> before, Map<String, Object> after, Map<String, Object> source, String op, Long ts_ms) {
        this.before = before;
        this.after = after;
        this.source = source;
        this.op = op;
        this.ts_ms = ts_ms;
    }

    public String getTopicName(String instance) {
        return String.join(
                ".",
                instance,
                source.getOrDefault("db", "default_schema").toString(),
                source.getOrDefault("table", "default_table").toString());
    }

    public Map<String, Object> getBefore() {
        return before;
    }

    public DmlMessage setBefore(Map<String, Object> before) {
        this.before = before;
        return this;
    }

    public Map<String, Object> getAfter() {
        return after;
    }

    public DmlMessage setAfter(Map<String, Object> after) {
        this.after = after;
        return this;
    }

    public Map<String, Object> getSource() {
        return source;
    }

    public DmlMessage setSource(Map<String, Object> source) {
        this.source = source;
        return this;
    }

    public String getOp() {
        return op;
    }

    public DmlMessage setOp(String op) {
        this.op = op;
        return this;
    }

    public Long getTs_ms() {
        return ts_ms;
    }

    public DmlMessage setTs_ms(Long ts_ms) {
        this.ts_ms = ts_ms;
        return this;
    }

    @Override
    public String toString() {
        return "DmlMessage{" +
                "before=" + before +
                ", after=" + after +
                ", source=" + source +
                ", op='" + op + '\'' +
                ", ts_ms=" + ts_ms +
                '}';
    }
}
