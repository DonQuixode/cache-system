package com.redis.base.DTO;

/**
 * DTO for POST requests to the KV API.
 */
public class PutRequest {
    private String key;
    private String valueType;
    private Object value;

    public PutRequest() {}

    public PutRequest(String key, String valueType, Object value) {
        this.key = key;
        this.valueType = valueType;
        this.value = value;
    }

    public String getKey() { return key; }
    public void setKey(String key) { this.key = key; }

    public String getValueType() { return valueType; }
    public void setValueType(String valueType) { this.valueType = valueType; }

    public Object getValue() { return value; }
    public void setValue(Object value) { this.value = value; }
}
