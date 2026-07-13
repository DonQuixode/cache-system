package com.redis.base.Service;

import com.redis.base.DTO.PutRequest;
import com.redis.base.Storage.KVStore;
import com.redis.base.Storage.RedisNodes;
import com.redis.base.Client.RedisClients;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;
import java.io.BufferedWriter;
import java.io.IOException;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;

@org.springframework.stereotype.Service
public class Service {

    private final KVStore kvStore;
    private final SentinelService sentinelService;
    private final RedisNodes redisNodes;
    private final RedisClients redisClients;
    private final ObjectMapper objectMapper;

    // Convenience constructor used by tests
    public Service(KVStore kvStore, SentinelService sentinelService) {
        this(kvStore, sentinelService, null, null, null);
    }

    @Autowired
    public Service(KVStore kvStore,
                   SentinelService sentinelService,
                   RedisNodes redisNodes,
                   RedisClients redisClients,
                   @Autowired(required = false) ObjectMapper objectMapper) {
        this.kvStore = kvStore;
        this.sentinelService = sentinelService;
        this.redisNodes = redisNodes;
        this.redisClients = redisClients;
        this.objectMapper = objectMapper;
    }

    /**
     * Persist an RDB snapshot to the local "rdb" file every 5 minutes.
     * Line format: key,valueType,value
     */
    @Scheduled(fixedRate = 300_000) // 5 minutes
    public void updateRDBSnapshot() {
        Path p = Paths.get("rdb");
        try (BufferedWriter bw = Files.newBufferedWriter(p, StandardCharsets.UTF_8)) {
            // strings
            for (Map.Entry<String, String> e : kvStore.getAllStrings().entrySet()) {
                String v = e.getValue() == null ? "" : e.getValue();
                bw.write(e.getKey() + ",string," + v);
                bw.newLine();
            }

            // integers
            for (Map.Entry<String, Integer> e : kvStore.getAllIntegers().entrySet()) {
                bw.write(e.getKey() + ",integer," + String.valueOf(e.getValue()));
                bw.newLine();
            }

            // lists
            for (Map.Entry<String, List<?>> e : kvStore.getAllLists().entrySet()) {
                String val;
                try {
                    val = objectMapper != null ? objectMapper.writeValueAsString(e.getValue()) : String.valueOf(e.getValue());
                } catch (Exception ex) {
                    val = String.valueOf(e.getValue());
                }
                bw.write(e.getKey() + ",list," + val);
                bw.newLine();
            }

            // sets
            for (Map.Entry<String, Set<?>> e : kvStore.getAllSets().entrySet()) {
                String val;
                try {
                    val = objectMapper != null ? objectMapper.writeValueAsString(e.getValue()) : String.valueOf(e.getValue());
                } catch (Exception ex) {
                    val = String.valueOf(e.getValue());
                }
                bw.write(e.getKey() + ",set," + val);
                bw.newLine();
            }

            // maps
            for (Map.Entry<String, Map<?, ?>> e : kvStore.getAllMaps().entrySet()) {
                String val;
                try {
                    val = objectMapper != null ? objectMapper.writeValueAsString(e.getValue()) : String.valueOf(e.getValue());
                } catch (Exception ex) {
                    val = String.valueOf(e.getValue());
                }
                bw.write(e.getKey() + ",map," + val);
                bw.newLine();
            }

        } catch (IOException ioe) {
            // best-effort snapshot; ignore failures silently
        }
    }

    // PutRequest moved to com.redis.base.PutRequest DTO

    /**
     * Put logic extracted from the controller. Returns the previous value (if any).
     * Throws IllegalArgumentException on bad input.
     */
    public Object put(PutRequest req) {
        if (req == null || req.getKey() == null || req.getValueType() == null) {
            throw new IllegalArgumentException("key and valueType are required");
        }

        String type = req.getValueType().trim().toLowerCase(Locale.ROOT);
        String key = req.getKey();

        // Move-on-put is handled inside KVStore.putX implementations
    Object result;
    switch (type) {
            case "string":
            case "str": {
                String s = req.getValue() == null ? null : String.valueOf(req.getValue());
                result = kvStore.putString(key, s);
                break;
            }
            case "integer":
            case "int": {
                Integer iv;
                Object v = req.getValue();
                if (v instanceof Number) {
                    iv = ((Number) v).intValue();
                } else if (v instanceof String) {
                    iv = Integer.valueOf((String) v);
                } else {
                    throw new IllegalArgumentException("value is not an integer");
                }
                result = kvStore.putInteger(key, iv);
                break;
            }
            case "list": {
                Object v = req.getValue();
                if (v instanceof List) {
                    result = kvStore.putList(key, (List<?>) v);
                    break;
                }
                throw new IllegalArgumentException("value must be a JSON array for type=list");
            }
            case "set": {
                Object v = req.getValue();
                if (v instanceof List) {
                    List<?> list = (List<?>) v;
                    Set<Object> set = new LinkedHashSet<>(list);
                    result = kvStore.putSet(key, set);
                    break;
                } else if (v instanceof Set) {
                    result = kvStore.putSet(key, (Set<?>) v);
                    break;
                }
                throw new IllegalArgumentException("value must be a JSON array for type=set");
            }
            case "map":
            case "dictionary":
            case "object": {
                Object v = req.getValue();
                if (v instanceof Map) {
                    result = kvStore.putMap(key, (Map<?, ?>) v);
                    break;
                }
                throw new IllegalArgumentException("value must be a JSON object for type=map");
            }
            default:
                throw new IllegalArgumentException("unknown valueType: " + req.getValueType());
        }

        // If this node is the master, notify slaves asynchronously about the put
        try {
            if (kvStore.isMaster()) {
                // fire-and-forget; SentinelService is @Async and returns a CompletableFuture
                sentinelService.sendToNodes(kvStore.getRedisNodes(), req);
            }
        } catch (Exception e) {
            // don't fail the primary put if slaves cannot be notified; just log
            // logging is omitted here to avoid adding a logger dependency; rethrow if desired
        }

        return result;
    }

    /**
     * Update nodes from a list of hostname/IP strings. Converts each to
     * InetAddress and uses the RedisSlaveNodes.add(List<InetAddress>) API.
     */
    public void updateNodes(List<String> addresses) {
        if (addresses == null) throw new IllegalArgumentException("addresses are required");
        List<InetAddress> addrs = new ArrayList<>();
        for (String s : addresses) {
            if (s == null) continue;
            try {
                addrs.add(InetAddress.getByName(s));
            } catch (UnknownHostException uhe) {
                throw new IllegalArgumentException("invalid address: " + s, uhe);
            }
        }
    kvStore.getRedisNodes().add(addrs);
    }

    /**
     * Set whether this node is the master. When true, this node will notify slaves on puts.
     */
    public void updateIsMaster(boolean isMaster) {
        kvStore.setMaster(isMaster);
        kvStore.getRedisNodes().updateMaster();
    }

    /**
     * Return whether this node is currently the master.
     */
    public boolean checkIfMaster() {
        return kvStore.isMaster();
    }

    /**
     * Get logic extracted from the controller. Returns a map with key,type,value.
     * Throws NoSuchElementException if key not found.
     */
    public Map<String, Object> get(String key) {
        if (key == null) throw new IllegalArgumentException("key is required");

        String recordedType = kvStore.getKeyType(key);
        if (recordedType == null) throw new NoSuchElementException("key not found");

        String t = recordedType.trim().toLowerCase(Locale.ROOT);
        Object val;
        switch (t) {
            case "string":
                val = kvStore.getString(key);
                break;
            case "integer":
                val = kvStore.getInteger(key);
                break;
            case "list":
                val = kvStore.getList(key);
                break;
            case "set":
                val = kvStore.getSet(key);
                break;
            case "map":
                val = kvStore.getMap(key);
                break;
            default:
                throw new IllegalStateException("unknown recorded type: " + recordedType);
        }

        if (val == null) throw new NoSuchElementException("key not found in its typed bucket");

        return Map.of("key", key, "type", t, "value", val);
    }

    /**
     * Register a client webhook address and return the current master/slave list.
     * Address may be in the form "host:port" or a full URL (http://host:port/path).
     */
    public List<Map<String, String>> registerClient(String address) {
        if (address == null || address.isBlank()) throw new IllegalArgumentException("address is required");
        if (redisClients == null || redisNodes == null) throw new IllegalStateException("RedisClients or RedisNodes not available in Service");

        // parse address into InetSocketAddress
        String working = address.trim();
        // strip scheme
        if (working.startsWith("http://")) working = working.substring(7);
        else if (working.startsWith("https://")) working = working.substring(8);
        // strip path
        int slash = working.indexOf('/');
        if (slash >= 0) working = working.substring(0, slash);

        String host;
        int port = 80;
        int colon = working.lastIndexOf(':');
        if (colon > 0) {
            host = working.substring(0, colon);
            try {
                port = Integer.parseInt(working.substring(colon + 1));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("invalid port in address: " + address, e);
            }
        } else {
            host = working;
        }

        InetSocketAddress isa = new InetSocketAddress(host, port);
        redisClients.putClient(isa);

        // build master/slave list
        Set<InetAddress> masters = redisNodes.getMasterNodes();
        List<Map<String, String>> list = new ArrayList<>();
        for (InetAddress addr : redisNodes.getAddresses()) {
            String h = addr.getHostAddress();
            String role = masters.contains(addr) ? "master" : "slave";
            list.add(Map.of("address", h, "role", role));
        }
        return list;
    }
}
