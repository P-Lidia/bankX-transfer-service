package com.bankx.transfer.shared.config;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import java.util.TimeZone;
/**
 * Единый ObjectMapper для всего сервиса.
 * - Даты как ISO-8601 (не timestamps)
 * - BigDecimal без научной нотации
 * - Игнорируем лишние поля во входящем JSON
 * - Используем UTC
 */
@Configuration
public class JacksonConfig {

        @Bean
        public ObjectMapper objectMapper() {
            ObjectMapper om = new ObjectMapper();
            om.registerModule(new JavaTimeModule());
            om.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
            om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            om.configure(SerializationFeature.WRITE_BIGDECIMAL_AS_PLAIN, true);
            om.setTimeZone(TimeZone.getTimeZone("UTC"));
            return om;
        }
    }


