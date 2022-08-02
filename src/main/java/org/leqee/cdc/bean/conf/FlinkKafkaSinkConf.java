package org.leqee.cdc.bean.conf;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchemaBuilder;
import org.apache.flink.connector.kafka.sink.KafkaSinkBuilder;
import org.apache.flink.connector.kafka.sink.TopicSelector;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.leqee.cdc.bean.message.DmlMessage;
import org.leqee.cdc.util.JsonConvertor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class FlinkKafkaSinkConf {
    @JsonProperty(value = "bootstrap-servers")
    private String bootstrapServers;
    @JsonProperty(value = "delivery-guarantee")
    private String deliveryGuarantee;
    @JsonProperty(value = "transactional-id-prefix")
    private String transactionalIdPrefix;
    @JsonProperty(value = "kafka-producer")
    private List<Map<String, String>> kafkaProducerConf;

    private static final Logger LOG = LoggerFactory.getLogger(FlinkKafkaSinkConf.class);

    public FlinkKafkaSinkConf() {
    }

    public FlinkKafkaSinkConf(String bootstrapServers, String deliveryGuarantee, String transactionalIdPrefix, List<Map<String, String>> kafkaProducerConf) {
        this.bootstrapServers = bootstrapServers;
        this.deliveryGuarantee = deliveryGuarantee;
        this.transactionalIdPrefix = transactionalIdPrefix;
        this.kafkaProducerConf = kafkaProducerConf;
    }

    public void applyConf(KafkaSinkBuilder<String> kafkaSinkBuilder, String instance) throws Exception {
        getBootstrapServers(kafkaSinkBuilder)
                .getDeliveryGuarantee(kafkaSinkBuilder)
                .getTransactionalIdPrefix(kafkaSinkBuilder)
                .getKafkaProducerConf(kafkaSinkBuilder)
                .getKafkaRecordSerializationSchema(kafkaSinkBuilder, instance);
    }

    public FlinkKafkaSinkConf getBootstrapServers(KafkaSinkBuilder<String> kafkaSinkBuilder) throws Exception {
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            String msg = "bootstrap-servers is required, now is missing";
            LOG.error(msg);
            throw new Exception(msg);
        } else {
            kafkaSinkBuilder.setBootstrapServers(bootstrapServers);
            return this;
        }
    }

    public FlinkKafkaSinkConf setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        return this;
    }

    public FlinkKafkaSinkConf getDeliveryGuarantee(KafkaSinkBuilder<String> kafkaSinkBuilder) {
        if (deliveryGuarantee == null || deliveryGuarantee.isEmpty()) {
            LOG.info("delivery-guarantee is missing, use default value `{}`", "EXACTLY_ONCE");
            kafkaSinkBuilder.setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE);
        } else {
            switch (deliveryGuarantee.toUpperCase(Locale.ROOT)) {
                case "AT_LEAST_ONCE" :
                    kafkaSinkBuilder.setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE);
                    break;
                case "EXACTLY_ONCE" :
                default:
                    kafkaSinkBuilder.setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE);
            }
        }
        return this;
    }

    public FlinkKafkaSinkConf setDeliveryGuarantee(String deliveryGuarantee) {
        this.deliveryGuarantee = deliveryGuarantee;
        return this;
    }

    public FlinkKafkaSinkConf getTransactionalIdPrefix(KafkaSinkBuilder<String> kafkaSinkBuilder) {
        if (transactionalIdPrefix == null || transactionalIdPrefix.isEmpty()) {
            final String defaultPrefix = "default-prefix";
            LOG.info("transactional-id-prefix is missing, use default value `{}`", defaultPrefix);
            kafkaSinkBuilder.setTransactionalIdPrefix(defaultPrefix);
        } else {
            kafkaSinkBuilder.setTransactionalIdPrefix(transactionalIdPrefix);
        }
        return this;
    }

    public FlinkKafkaSinkConf setTransactionalIdPrefix(String transactionalIdPrefix) {
        this.transactionalIdPrefix = transactionalIdPrefix;
        return this;
    }

    public FlinkKafkaSinkConf getKafkaProducerConf(KafkaSinkBuilder<String> kafkaSinkBuilder) {
        if (kafkaProducerConf != null && !kafkaProducerConf.isEmpty()) {
            Properties kafkaProperties = new Properties();
            kafkaProducerConf.forEach(map -> map.forEach(kafkaProperties::setProperty));
            kafkaSinkBuilder.setKafkaProducerConfig(kafkaProperties);
        }
        return this;
    }

    public FlinkKafkaSinkConf setKafkaProducerConf(List<Map<String, String>> kafkaProducerConf) {
        this.kafkaProducerConf = kafkaProducerConf;
        return this;
    }

    public FlinkKafkaSinkConf getKafkaRecordSerializationSchema(KafkaSinkBuilder<String> kafkaSinkBuilder, String instance) {
        KafkaRecordSerializationSchemaBuilder<String> kafkaRecordSerializationSchemaBuilder
        = KafkaRecordSerializationSchema.builder()
                .setTopicSelector((TopicSelector<String>) json -> {
                    Optional<DmlMessage> optionalDmlMessage = JsonConvertor.convertFromJsonString(json, DmlMessage.class);
                    if (optionalDmlMessage.isPresent()) {
                        return optionalDmlMessage.get().getTopicName(instance);
                    } else {
                        return String.join(
                                ".",
                                instance,
                                "default_schema",
                                "default_table"
                        );
                    }
                })
                .setKeySerializationSchema(new SimpleStringSchema())
                .setValueSerializationSchema(new SimpleStringSchema());
        kafkaSinkBuilder.setRecordSerializer(kafkaRecordSerializationSchemaBuilder.build());
        return this;
    }

    @Override
    public String toString() {
        return "FlinkKafkaSinkConf{" +
                "bootstrapServers='" + bootstrapServers + '\'' +
                ", deliveryGuarantee='" + deliveryGuarantee + '\'' +
                ", transactionalIdPrefix='" + transactionalIdPrefix + '\'' +
                ", kafkaProducerConf=" + kafkaProducerConf +
                '}';
    }
}
