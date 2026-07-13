package com.redis.base.Storage;

import java.util.Objects;
import java.util.Set;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;
import java.io.IOException;
import jakarta.annotation.PostConstruct;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.springframework.stereotype.Component;

import com.redis.base.Client.RedisClients;

import org.springframework.beans.factory.annotation.Autowired;

/**
 * Thread-safe in-memory key-value store with typed buckets for common value types.
 *
 * This store exposes five separate maps, all keyed by {@code String}:
 * - strings: {@code String}
 * - integers: {@code Integer}
 * - lists: {@code List<?>} (keeps the original List implementation)
 * - sets: {@code Set<?>} (keeps the original Set implementation)
 * - maps: {@code Map<?,?>} (keeps the original Map implementation)
 *
 * Note: Java collections cannot hold primitives directly; primitive values will be boxed
 * (e.g. int -> Integer) when stored in the List/Set/Map collections.
 */
@Component
public final class KVStore {
    // typed buckets
    private final ConcurrentMap<String, String> strings = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Integer> integers = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, List<?>> lists = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Set<?>> sets = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Map<?, ?>> maps = new ConcurrentHashMap<>();

    /**
     * overall map that tracks which typed-bucket a given key currently lives in.
     * value is one of "string","integer","list","set","map".
     */
    private final ConcurrentMap<String, String> keyTypes = new ConcurrentHashMap<>();

    private static final String TYPE_STRING = "string";
    private static final String TYPE_INTEGER = "integer";
    private static final String TYPE_LIST = "list";
    private static final String TYPE_SET = "set";
    private static final String TYPE_MAP = "map";

    // whether this node should act as master (if true, it will notify slaves on puts)
    private volatile boolean isMaster = false;

    // reference to the singleton RedisNodes container for updating addresses
    private final RedisNodes redisNodes;

    private final RedisClients redisClients;

    // Optional ObjectMapper used for RDB (de)serialization if available in context
    private final ObjectMapper objectMapper;

    @Autowired
    public KVStore(RedisNodes redisNodes, RedisClients redisClients, @Autowired(required = false) ObjectMapper objectMapper) {
        this.redisNodes = redisNodes;
        this.redisClients = redisClients;
        this.objectMapper = objectMapper;
    }

    @PostConstruct
    private void init() {
        readRDB();
        updateNodes();
    }

    // --- String API ---
    public String putString(String key, String value) {
        Objects.requireNonNull(key, "key must not be null");
        // move key from previous type if necessary
        String prevType = keyTypes.get(key);
        Object prevRemoved = null;
        if (prevType != null && !TYPE_STRING.equals(prevType)) {
            prevRemoved = removeFromType(key, prevType);
        }
        keyTypes.put(key, TYPE_STRING);
        String prev = strings.put(key, value);
        return prevRemoved != null ? String.valueOf(prevRemoved) : prev;
    }

    public String getString(String key) {
        Objects.requireNonNull(key, "key must not be null");
        String t = keyTypes.get(key);
        if (!TYPE_STRING.equals(t)) return null;
        return strings.get(key);
    }
    
    public String removeString(String key) {
        Objects.requireNonNull(key, "key must not be null");
        String prev = strings.remove(key);
        keyTypes.remove(key, TYPE_STRING);
        return prev;
    }

    // --- Integer API ---
    public Integer putInteger(String key, Integer value) {
        Objects.requireNonNull(key, "key must not be null");
        String prevType = keyTypes.get(key);
        Object prevRemoved = null;
        if (prevType != null && !TYPE_INTEGER.equals(prevType)) {
            prevRemoved = removeFromType(key, prevType);
        }
        keyTypes.put(key, TYPE_INTEGER);
        Integer prev = integers.put(key, value);
        return prevRemoved instanceof Integer ? (Integer) prevRemoved : prev;
    }

    public Integer getInteger(String key) {
        Objects.requireNonNull(key, "key must not be null");
        String t = keyTypes.get(key);
        if (!TYPE_INTEGER.equals(t)) return null;
        return integers.get(key);
    }
    
    public Integer removeInteger(String key) {
        Objects.requireNonNull(key, "key must not be null");
        Integer prev = integers.remove(key);
        keyTypes.remove(key, TYPE_INTEGER);
        return prev;
    }

    // --- List API ---
    public List<?> putList(String key, List<?> list) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(list, "list must not be null");
        String prevType = keyTypes.get(key);
        Object prevRemoved = null;
        if (prevType != null && !TYPE_LIST.equals(prevType)) {
            prevRemoved = removeFromType(key, prevType);
        }
        keyTypes.put(key, TYPE_LIST);
        List<?> prev = lists.put(key, list);
        return prevRemoved instanceof List ? (List<?>) prevRemoved : prev;
    }

    @SuppressWarnings("unchecked")
    public <T> List<T> getList(String key) {
        Objects.requireNonNull(key, "key must not be null");
        String t = keyTypes.get(key);
        if (!TYPE_LIST.equals(t)) return null;
        return (List<T>) lists.get(key);
    }

    // --- Set API ---
    public Set<?> putSet(String key, Set<?> set) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(set, "set must not be null");
        String prevType = keyTypes.get(key);
        Object prevRemoved = null;
        if (prevType != null && !TYPE_SET.equals(prevType)) {
            prevRemoved = removeFromType(key, prevType);
        }
        keyTypes.put(key, TYPE_SET);
        Set<?> prev = sets.put(key, set);
        return prevRemoved instanceof Set ? (Set<?>) prevRemoved : prev;
    }

    @SuppressWarnings("unchecked")
    public <T> Set<T> getSet(String key) {
        Objects.requireNonNull(key, "key must not be null");
        String t = keyTypes.get(key);
        if (!TYPE_SET.equals(t)) return null;
        return (Set<T>) sets.get(key);
    }

    // --- Map-of-maps API ---
    public Map<?, ?> putMap(String key, Map<?, ?> map) {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(map, "map must not be null");
        String prevType = keyTypes.get(key);
        Object prevRemoved = null;
        if (prevType != null && !TYPE_MAP.equals(prevType)) {
            prevRemoved = removeFromType(key, prevType);
        }
        keyTypes.put(key, TYPE_MAP);
        Map<?, ?> prev = maps.put(key, map);
        return prevRemoved instanceof Map ? (Map<?, ?>) prevRemoved : prev;
    }

    public Map<?, ?> removeMap(String key) {
        Objects.requireNonNull(key, "key must not be null");
        Map<?, ?> prev = maps.remove(key);
        keyTypes.remove(key, TYPE_MAP);
        return prev;
    }

    public List<?> removeList(String key) {
        Objects.requireNonNull(key, "key must not be null");
        List<?> prev = lists.remove(key);
        keyTypes.remove(key, TYPE_LIST);
        return prev;
    }

    public Set<?> removeSet(String key) {
        Objects.requireNonNull(key, "key must not be null");
        Set<?> prev = sets.remove(key);
        keyTypes.remove(key, TYPE_SET);
        return prev;
    }

    @SuppressWarnings("unchecked")
    public <K, V> Map<K, V> getMap(String key) {
        Objects.requireNonNull(key, "key must not be null");
        String t = keyTypes.get(key);
        if (!TYPE_MAP.equals(t)) return null;
        return (Map<K, V>) maps.get(key);
    }

    // remove key from the given typed-bucket and return the removed value (or null)
    private Object removeFromType(String key, String type) {
        if (type == null) return null;
        switch (type) {
            case TYPE_STRING:
                return strings.remove(key);
            case TYPE_INTEGER:
                return integers.remove(key);
            case TYPE_LIST:
                return lists.remove(key);
            case TYPE_SET:
                return sets.remove(key);
            case TYPE_MAP:
                return maps.remove(key);
            default:
                return null;
        }
    }

    // --- Utilities ---
    public void clearAll() {
        strings.clear();
        integers.clear();
        lists.clear();
        sets.clear();
        maps.clear();
        keyTypes.clear();
    }

    /**
     * Return the recorded type for a given key, or null if the key is not present.
     * The returned value is one of the TYPE_* constants (e.g. "string", "list").
     */
    public String getKeyType(String key) {
        Objects.requireNonNull(key, "key must not be null");
        return keyTypes.get(key);
    }

    // Snapshot accessors (return shallow copies of the internal maps)
    public Map<String, String> getAllStrings() { return new java.util.HashMap<>(strings); }
    public Map<String, Integer> getAllIntegers() { return new java.util.HashMap<>(integers); }
    public Map<String, List<?>> getAllLists() { return new java.util.HashMap<>(lists); }
    public Map<String, Set<?>> getAllSets() { return new java.util.HashMap<>(sets); }
    public Map<String, Map<?, ?>> getAllMaps() { return new java.util.HashMap<>(maps); }

    /**
     * Calls all the nodes provided to update their node addresses with its own.
     */
    public void updateNodes() {
        //implemented later.
    }

    /**
     * Read an on-disk RDB file (named "rdb" in the working directory) and populate
     * the in-memory typed buckets. Expected line format per entry:
     * key,valueType,value
     * where valueType is one of: string, integer, list, set, map
     */
    public void readRDB() {
        Path p = Paths.get("rdb");
        if (!Files.exists(p)) return;
        try {
            for (String raw : Files.readAllLines(p, StandardCharsets.UTF_8)) {
                if (raw == null) continue;
                String line = raw.trim();
                if (line.isEmpty()) continue;
                int c1 = line.indexOf(',');
                if (c1 < 0) continue;
                int c2 = line.indexOf(',', c1 + 1);
                if (c2 < 0) continue;
                String key = line.substring(0, c1).trim();
                String type = line.substring(c1 + 1, c2).trim().toLowerCase(java.util.Locale.ROOT);
                String val = line.substring(c2 + 1).trim();

                try {
                    switch (type) {
                        case TYPE_STRING:
                            putString(key, unquote(val));
                            break;
                        case TYPE_INTEGER:
                            try {
                                Integer iv = Integer.valueOf(unquote(val));
                                putInteger(key, iv);
                            } catch (NumberFormatException nfe) {
                                // skip malformed integer
                            }
                            break;
                        case TYPE_LIST: {
                            List<?> list = null;
                            if (objectMapper != null) {
                                try {
                                    list = objectMapper.readValue(val, List.class);
                                } catch (Exception e) {
                                    list = null;
                                }
                            }
                            if (list == null) list = simpleParseList(val);
                            putList(key, list);
                            break;
                        }
                        case TYPE_SET: {
                            Set<?> set = null;
                            if (objectMapper != null) {
                                try {
                                    List<?> tmp = objectMapper.readValue(val, List.class);
                                    set = new java.util.LinkedHashSet<>(tmp);
                                } catch (Exception e) {
                                    set = null;
                                }
                            }
                            if (set == null) set = new java.util.LinkedHashSet<>(simpleParseList(val));
                            putSet(key, set);
                            break;
                        }
                        case TYPE_MAP: {
                            Map<?, ?> map = null;
                            if (objectMapper != null) {
                                try {
                                    map = objectMapper.readValue(val, Map.class);
                                } catch (Exception e) {
                                    map = null;
                                }
                            }
                            if (map == null) map = simpleParseMap(val);
                            putMap(key, map);
                            break;
                        }
                        default:
                            // unknown type - ignore
                    }
                } catch (Exception ignored) {
                    // be robust when reading on-startup
                }
            }
        } catch (IOException ioe) {
            // best-effort: ignore errors on startup
        }
    }

    private String unquote(String s) {
        if (s == null) return null;
        String t = s.trim();
        if ((t.startsWith("\"") && t.endsWith("\"")) || (t.startsWith("'") && t.endsWith("'"))) {
            return t.substring(1, t.length() - 1);
        }
        return t;
    }

    private List<String> simpleParseList(String s) {
        String t = s == null ? "" : s.trim();
        if (t.startsWith("[") && t.endsWith("]")) {
            t = t.substring(1, t.length() - 1);
        }
        if (t.isEmpty()) return java.util.Collections.emptyList();
        String[] parts = t.split(",");
        List<String> out = new java.util.ArrayList<>();
        for (String p : parts) out.add(unquote(p.trim()));
        return out;
    }

    private Map<String, String> simpleParseMap(String s) {
        String t = s == null ? "" : s.trim();
        if (t.startsWith("{" ) && t.endsWith("}")) {
            t = t.substring(1, t.length() - 1);
        }
        Map<String, String> out = new java.util.LinkedHashMap<>();
        if (t.isEmpty()) return out;
        String[] parts = t.split(",");
        for (String p : parts) {
            int idx = p.indexOf(':');
            if (idx <= 0) continue;
            String k = unquote(p.substring(0, idx).trim());
            String v = unquote(p.substring(idx + 1).trim());
            out.put(k, v);
        }
        return out;
    }

    public int sizeStrings() { return strings.size(); }
    public int sizeIntegers() { return integers.size(); }
    public int sizeLists() { return lists.size(); }
    public int sizeSets() { return sets.size(); }
    public int sizeMaps() { return maps.size(); }

    // --- master flag accessor ---
    /*
        Only call setMaster() from Service's updateIsMaster.
     */
    public boolean isMaster() { return isMaster; }
    public void setMaster(boolean isMaster) { this.isMaster = isMaster; }

    /** expose the singleton RedisNodes container held by this store */
    public RedisNodes getRedisNodes() { return this.redisNodes; }

    public RedisClients getRedisClients() { return this.redisClients; }
}
