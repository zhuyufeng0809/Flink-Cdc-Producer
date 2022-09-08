package org.awesome.flink.bean.conf;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class FlinkCdcSourceConf {
    private String instance;
    private String hostname;
    private String username;
    private String password;
    private Integer port;
    @JsonProperty(value = "database-list")
    private List<String> databaseList;
    @JsonProperty(value = "table-list")
    private List<String> tableList;
    @JsonProperty(value = "include-schema-change")
    private Boolean includeSchemaChange;
    @JsonProperty(value = "server-id")
    private String serverId;
    @JsonProperty(value = "scan-startup-mode")
    private ScanStartupMode scanStartupMode;
    @JsonProperty(value = "server-time-zone")
    private String serverTimeZone;
    @JsonProperty(value = "connect-timeout")
    private String connectTimeout;
    @JsonProperty(value = "connect-max-retries")
    private Integer connectMaxRetries;
    @JsonProperty(value = "connection-pool-size")
    private Integer connectionPoolSize;
    @JsonProperty(value = "heartbeat-interval")
    private String heartbeatInterval;
    private List<Map<String, String>> jdbc;
    private List<Map<String, String>> debezium;

    private static final Logger LOG = LoggerFactory.getLogger(FlinkCdcSourceConf.class);

    public FlinkCdcSourceConf() {
    }

    public FlinkCdcSourceConf(String instance, String hostname, String username, String password, Integer port, List<String> databaseList, List<String> tableList, Boolean includeSchemaChange, String serverId, ScanStartupMode scanStartupMode, String serverTimeZone, String connectTimeout, Integer connectMaxRetries, Integer connectionPoolSize, String heartbeatInterval, List<Map<String, String>> jdbc, List<Map<String, String>> debezium) {
        this.instance = instance;
        this.hostname = hostname;
        this.username = username;
        this.password = password;
        this.port = port;
        this.databaseList = databaseList;
        this.tableList = tableList;
        this.includeSchemaChange = includeSchemaChange;
        this.serverId = serverId;
        this.scanStartupMode = scanStartupMode;
        this.serverTimeZone = serverTimeZone;
        this.connectTimeout = connectTimeout;
        this.connectMaxRetries = connectMaxRetries;
        this.connectionPoolSize = connectionPoolSize;
        this.heartbeatInterval = heartbeatInterval;
        this.jdbc = jdbc;
        this.debezium = debezium;
    }

    public void applyConf(MySqlSourceBuilder<String> mySqlSourceBuilder) throws Exception {
        getHostname(mySqlSourceBuilder)
                .getUsername(mySqlSourceBuilder)
                .getPassword(mySqlSourceBuilder)
                .getPort(mySqlSourceBuilder)
                .getDatabaseList(mySqlSourceBuilder)
                .getTableList(mySqlSourceBuilder)
                .getIncludeSchemaChange(mySqlSourceBuilder)
                .getServerId(mySqlSourceBuilder)
                .getScanStartupMode(mySqlSourceBuilder)
                .getServerTimeZone(mySqlSourceBuilder)
                .getConnectTimeout(mySqlSourceBuilder)
                .getConnectMaxRetries(mySqlSourceBuilder)
                .getConnectionPoolSize(mySqlSourceBuilder)
                .getHeartbeatInterval(mySqlSourceBuilder)
                .getJdbc(mySqlSourceBuilder)
                .getDebezium(mySqlSourceBuilder)
                .getDeserializer(mySqlSourceBuilder);
    }

    public String getInstance() {
        return instance;
    }

    public FlinkCdcSourceConf setInstance(String instance) {
        this.instance = instance;
        return this;
    }

    public FlinkCdcSourceConf getHostname(MySqlSourceBuilder<String> mySqlSourceBuilder) throws Exception {
        if (hostname == null || hostname.isEmpty()) {
            String msg = "hostname is required, now is missing";
            LOG.error(msg);
            throw new Exception(msg);
        } else {
            mySqlSourceBuilder.hostname(hostname);
            return this;
        }
    }

    public FlinkCdcSourceConf setHostname(String hostname) {
        this.hostname = hostname;
        return this;
    }

    public FlinkCdcSourceConf getUsername(MySqlSourceBuilder<String> mySqlSourceBuilder) throws Exception {
        if (username == null || username.isEmpty()) {
            String msg = "username is required, now is missing";
            LOG.error(msg);
            throw new Exception(msg);
        } else {
            mySqlSourceBuilder.username(username);
            return this;
        }
    }

    public FlinkCdcSourceConf setUsername(String username) {
        this.username = username;
        return this;
    }

    public FlinkCdcSourceConf getPassword(MySqlSourceBuilder<String> mySqlSourceBuilder) throws Exception {
        if (password == null || password.isEmpty()) {
            String msg = "password is required, now is missing";
            LOG.error(msg);
            throw new Exception(msg);
        } else {
            mySqlSourceBuilder.password(password);
            return this;
        }
    }

    public FlinkCdcSourceConf setPassword(String password) {
        this.password = password;
        return this;
    }

    public FlinkCdcSourceConf getPort(MySqlSourceBuilder<String> mySqlSourceBuilder) {
        if (port != null) {
            mySqlSourceBuilder.port(port);
        } // MySqlSourceBuilder default uses 3306
        return this;
    }

    public FlinkCdcSourceConf setPort(Integer port) {
        this.port = port;
        return this;
    }

    public FlinkCdcSourceConf getDatabaseList(MySqlSourceBuilder<String> mySqlSourceBuilder) {
        if (databaseList == null || databaseList.isEmpty()) {
            LOG.info("database-list is missing, use default value `{}`", ".*");
            mySqlSourceBuilder.databaseList(".*");
        } else {
            mySqlSourceBuilder.databaseList(databaseList.toArray(databaseList.toArray(new String[0])));
        }
        return this;
    }

    public FlinkCdcSourceConf setDatabaseList(List<String> databaseList) {
        this.databaseList = databaseList;
        return this;
    }

    public FlinkCdcSourceConf getTableList(MySqlSourceBuilder<String> mySqlSourceBuilder) {
        if (tableList == null || tableList.isEmpty()) {
            LOG.info("table-list is missing, use default value `{}`", ".*");
            mySqlSourceBuilder.tableList(".*");
        } else {
            mySqlSourceBuilder.tableList(tableList.toArray(tableList.toArray(new String[0])));
        }
        return this;
    }

    public FlinkCdcSourceConf setTableList(List<String> tableList) {
        this.tableList = tableList;
        return this;
    }

    public FlinkCdcSourceConf getIncludeSchemaChange(MySqlSourceBuilder<String> mySqlSourceBuilder) {
        if (includeSchemaChange != null) {
            mySqlSourceBuilder.includeSchemaChanges(includeSchemaChange);
        } // MySqlSourceBuilder default excludes schema change
        return this;
    }

    public FlinkCdcSourceConf setIncludeSchemaChange(Boolean includeSchemaChange) {
        this.includeSchemaChange = includeSchemaChange;
        return this;
    }

    public FlinkCdcSourceConf getServerId(MySqlSourceBuilder<String> mySqlSourceBuilder) {
        if (serverId != null && !serverId.isEmpty()) {
            mySqlSourceBuilder.serverId(serverId);
        }
        return this;
    }

    public FlinkCdcSourceConf setServerId(String serverId) {
        this.serverId = serverId;
        return this;
    }

    public FlinkCdcSourceConf getScanStartupMode(MySqlSourceBuilder<String> mySqlSourceBuilder) {
        if (scanStartupMode == null) {
            LOG.info("scan-startup-mode is missing, use default value `{}`", "latest");
            mySqlSourceBuilder.startupOptions(ScanStartupMode.getDefaultStartupOptions());
        } else {
            mySqlSourceBuilder.startupOptions(scanStartupMode.getStartupOptions());
        }
        return this;
    }

    public FlinkCdcSourceConf setScanStartupMode(ScanStartupMode scanStartupMode) {
        this.scanStartupMode = scanStartupMode;
        return this;
    }

    public FlinkCdcSourceConf getServerTimeZone(MySqlSourceBuilder<String> mySqlSourceBuilder) {
        if (serverTimeZone != null && !serverTimeZone.isEmpty()) {
            mySqlSourceBuilder.serverTimeZone(serverTimeZone);
        } // MySqlSourceBuilder default uses UTC
        return this;
    }

    public FlinkCdcSourceConf setServerTimeZone(String serverTimeZone) {
        this.serverTimeZone = serverTimeZone;
        return this;
    }

    public FlinkCdcSourceConf getConnectTimeout(MySqlSourceBuilder<String> mySqlSourceBuilder) {
        if (connectTimeout != null && !connectTimeout.isEmpty()) {
            mySqlSourceBuilder.connectTimeout(Duration.parse(connectTimeout));
        } // MySqlSourceBuilder default uses 30s
        return this;
    }

    public FlinkCdcSourceConf setConnectTimeout(String connectTimeout) {
        this.connectTimeout = connectTimeout;
        return this;
    }

    public FlinkCdcSourceConf getConnectMaxRetries(MySqlSourceBuilder<String> mySqlSourceBuilder) {
        if (connectMaxRetries != null) {
            mySqlSourceBuilder.connectMaxRetries(connectMaxRetries);
        } // MySqlSourceBuilder default retries 3 times
        return this;
    }

    public FlinkCdcSourceConf setConnectMaxRetries(Integer connectMaxRetries) {
        this.connectMaxRetries = connectMaxRetries;
        return this;
    }

    public FlinkCdcSourceConf getConnectionPoolSize(MySqlSourceBuilder<String> mySqlSourceBuilder) {
        if (connectionPoolSize != null) {
            mySqlSourceBuilder.connectionPoolSize(connectionPoolSize);
        } // MySqlSourceBuilder default use 20
        return this;
    }

    public FlinkCdcSourceConf setConnectionPoolSize(Integer connectionPoolSize) {
        this.connectionPoolSize = connectionPoolSize;
        return this;
    }

    public FlinkCdcSourceConf getHeartbeatInterval(MySqlSourceBuilder<String> mySqlSourceBuilder) {
        if (heartbeatInterval != null && !heartbeatInterval.isEmpty()) {
            mySqlSourceBuilder.heartbeatInterval(Duration.parse(heartbeatInterval));
        } // MySqlSourceBuilder default use 30s
        return this;
    }

    public FlinkCdcSourceConf setHeartbeatInterval(String heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
        return this;
    }

    public FlinkCdcSourceConf getJdbc(MySqlSourceBuilder<String> mySqlSourceBuilder) {
        if (jdbc != null && !jdbc.isEmpty()) {
            Properties jdbcProperties = new Properties();
            jdbc.forEach(map -> map.forEach(jdbcProperties::setProperty));
            mySqlSourceBuilder.jdbcProperties(jdbcProperties);
        }
        return this;
    }

    public FlinkCdcSourceConf setJdbc(List<Map<String, String>> jdbc) {
        this.jdbc = jdbc;
        return this;
    }

    public FlinkCdcSourceConf getDebezium(MySqlSourceBuilder<String> mySqlSourceBuilder) {
        if (debezium != null && !debezium.isEmpty()) {
            Properties debeziumProperties = new Properties();
            debezium.forEach(map -> map.forEach(debeziumProperties::setProperty));
            mySqlSourceBuilder.jdbcProperties(debeziumProperties);
        }
        return this;
    }

    public FlinkCdcSourceConf setDebezium(List<Map<String, String>> debezium) {
        this.debezium = debezium;
        return this;
    }

    public FlinkCdcSourceConf getDeserializer(MySqlSourceBuilder<String> mySqlSourceBuilder) {
        mySqlSourceBuilder.deserializer(new JsonDebeziumDeserializationSchema());
        return this;
    }

    @Override
    public String toString() {
        return "FlinkCdcSourceConf{" +
                "instance='" + instance + '\'' +
                ", hostname='" + hostname + '\'' +
                ", username='" + username + '\'' +
                ", password='" + password + '\'' +
                ", port=" + port +
                ", databaseList=" + databaseList +
                ", tableList=" + tableList +
                ", includeSchemaChange=" + includeSchemaChange +
                ", serverId='" + serverId + '\'' +
                ", scanStartupMode=" + scanStartupMode +
                ", serverTimeZone='" + serverTimeZone + '\'' +
                ", connectTimeout='" + connectTimeout + '\'' +
                ", connectMaxRetries='" + connectMaxRetries + '\'' +
                ", connectionPoolSize='" + connectionPoolSize + '\'' +
                ", heartbeatInterval='" + heartbeatInterval + '\'' +
                ", jdbc=" + jdbc +
                ", debezium=" + debezium +
                '}';
    }
}
