package org.leqee.cdc.bean.conf;

import com.ververica.cdc.connectors.mysql.table.StartupOptions;

import java.util.Locale;

public class ScanStartupMode {
    private String mode;
    private String file;
    private String offset;
    private String timestamp;

    public ScanStartupMode() {
    }

    public ScanStartupMode(String mode, String file, String offset, String timestamp) {
        this.mode = mode;
        this.file = file;
        this.offset = offset;
        this.timestamp = timestamp;
    }

    public String getMode() {
        return mode;
    }

    public ScanStartupMode setMode(String mode) {
        this.mode = mode;
        return this;
    }

    public String getFile() {
        return file;
    }

    public ScanStartupMode setFile(String file) {
        this.file = file;
        return this;
    }

    public int getOffset() {
        return Integer.parseInt(offset);
    }

    public ScanStartupMode setOffset(String offset) {
        this.offset = offset;
        return this;
    }

    public long getTimestamp() {
        return Long.parseLong(timestamp);
    }

    public ScanStartupMode setTimestamp(String timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public StartupOptions getStartupOptions() {
        switch (getMode().toLowerCase(Locale.ROOT)) {
            case "initial" : return initialMode();
            case "earliest" : return earliestMode();
            case "specific-offset" : return specificOffsetMode(getFile(), getOffset());
            case "specific-timestamp" : return specificTimestampMode(getTimestamp());
            case "latest" :
            default: return latestMode();
        }
    }

    public static StartupOptions getDefaultStartupOptions() {
        return latestMode();
    }

    private static StartupOptions initialMode() {
        return StartupOptions.initial();
    }

    private static StartupOptions earliestMode() {
        return StartupOptions.earliest();
    }

    private static StartupOptions latestMode() {
        return StartupOptions.latest();
    }

    private static StartupOptions specificOffsetMode(String specificOffsetFile, int specificOffsetPos) {
        return StartupOptions.specificOffset(specificOffsetFile, specificOffsetPos);
    }

    private static StartupOptions specificTimestampMode(long startupTimestampMillis) {
        return StartupOptions.timestamp(startupTimestampMillis);
    }
}
