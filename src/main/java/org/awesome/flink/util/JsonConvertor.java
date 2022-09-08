package org.awesome.flink.util;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

public class JsonConvertor {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .configure(JsonParser.Feature.ALLOW_COMMENTS,true)
            .configure(JsonParser.Feature.ALLOW_YAML_COMMENTS,true)
            .configure(JsonParser.Feature.ALLOW_MISSING_VALUES,true)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);

    public static <T> T convertFromJsonFile(String jsonFilepath, Class<T> clazz) throws Exception {
        return OBJECT_MAPPER.readValue(new File(jsonFilepath), clazz);
    }

    public static <T> Optional<T> convertFromJsonString(String json, Class<T> clazz) {
        try {
            return Optional.ofNullable(OBJECT_MAPPER.readValue(json, clazz));
        } catch (IOException e) {
            e.printStackTrace();
            return Optional.empty();
        }
    }
}
