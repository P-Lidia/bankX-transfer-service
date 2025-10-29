package com.bankx.transfer.shared.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

/**
 * Утилита для централизованной работы с JSON во всём сервисе.
 * Назначение:
 * - Единая точка сериализации/десериализации поверх глобально настроенного ObjectMapper.
 * - Упрощает работу с JSON в трёх ключевых местах Transfer Service:
 * 1) Outbox Pattern: сборка payload в JSON/JsonNode для колонки JSONB (PostgreSQL).
 * 2) Kafka: подготовка/разбор строкового JSON при необходимости (raw-публикация или отладка).
 * 3) REST: вспомогательные преобразования
 */
@Component
public class JsonConverter {

    private final ObjectMapper om;

    public JsonConverter(ObjectMapper om) {
        this.om = om;
    }

    public String toJson(Object value) {
        try {
            return om.writeValueAsString(value);
        } catch (Exception e) {
            throw new RuntimeException("JSON write error", e);
        }
    }

    public JsonNode toJsonNode(Object value) {
        return om.valueToTree(value);
    }

    public <T> T fromJson(String json, Class<T> type) {
        try {
            return om.readValue(json, type);
        } catch (Exception e) {
            throw new RuntimeException("JSON read error", e);
        }
    }

    public <T> T fromJson(String json, TypeReference<T> ref) {
        try {
            return om.readValue(json, ref);
        } catch (Exception e) {
            throw new RuntimeException("JSON read error", e);
        }
    }
}