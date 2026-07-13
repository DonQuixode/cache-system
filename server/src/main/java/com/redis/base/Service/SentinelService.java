package com.redis.base.Service;

import com.redis.base.DTO.PutRequest;
import com.redis.base.Storage.RedisNodes;
import com.redis.base.Client.RedisClients;
import org.springframework.scheduling.annotation.Scheduled;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Async;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.net.InetAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

@Service
public class SentinelService {

    private static final Logger log = LoggerFactory.getLogger(SentinelService.class);

    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;
    private final RedisNodes redisNodes;
    private final RedisClients redisClients;

    @Autowired
    public SentinelService(@Autowired(required = false) ObjectMapper objectMapper,
                           RedisNodes redisNodes,
                           RedisClients redisClients) {
        this.objectMapper = objectMapper != null ? objectMapper : new ObjectMapper();
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(2))
                .version(HttpClient.Version.HTTP_1_1)
                .build();
        this.redisNodes = redisNodes;
        this.redisClients = redisClients;
    }

    // Stub for quorum decision; to be implemented later.
    private void quorum(InetAddress failed) {
        //Implements Raft Algorithm
        log.warn("Quorum check triggered for {} (stub)", failed.getHostAddress());
    }

    /**
     * Scheduled health-check that runs every 2 seconds.
     * If any node fails its health check, trigger quorum, refresh master list,
     * build the master/slave JSON array and POST it to registered webhooks.
     */
    @Scheduled(fixedRate = 2000)
    public void healthCheck() {
        List<InetAddress> addrs = redisNodes.getAddresses();
        for (InetAddress addr : addrs) {
            String host = addr.getHostAddress();
            String url = "http://" + host + ":8080/v1/healthcheck";
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(2))
                    .GET()
                    .build();
            boolean healthy = true;
            try {
                HttpResponse<Void> resp = httpClient.send(request, HttpResponse.BodyHandlers.discarding());
                if (resp.statusCode() / 100 != 2) {
                    healthy = false;
                }
            } catch (Exception e) {
                healthy = false;
            }

            if (!healthy) {
                // handle failure
                notifyAdmin(addr);
                quorum(addr);
                redisNodes.updateMaster();
                String json = buildMasterSlaveJsonArray();
                webHook(json);
            }
        }
    }

    /**
     * Build the JSON array of address/role objects from the current nodes state.
     */
    private String buildMasterSlaveJsonArray() {
        List<Map<String, String>> list = new ArrayList<>();
        Set<InetAddress> masters = redisNodes.getMasterNodes();
        for (InetAddress addr : redisNodes.getAddresses()) {
            String host = addr.getHostAddress();
            String role = masters.contains(addr) ? "master" : "slave";
            list.add(Map.of("address", host, "role", role));
        }
        try {
            return objectMapper.writeValueAsString(list);
        } catch (Exception e) {
            log.warn("Failed to serialize master/slave list", e);
            return "[]";
        }
    }

    /**
     * Send the JSON payload to all registered webhook clients.
     */
    private void webHook(String json) {
        List<String> clients = redisClients.getClients();
        for (String target : clients) {
            String url = target.startsWith("http") ? target : ("http://" + target);
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .timeout(Duration.ofSeconds(3))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(json))
                    .build();
            httpClient.sendAsync(req, HttpResponse.BodyHandlers.discarding())
                    .whenComplete((r, ex) -> {
                        if (ex != null) {
                            log.warn("Failed to call webhook {}: {}", url, ex.getMessage());
                        } else if (r.statusCode() >= 400) {
                            log.warn("Webhook {} returned HTTP {}", url, r.statusCode());
                        } else {
                            log.debug("Successfully called webhook {}", url);
                        }
                    });
        }
    }

    @Async
    private void notifyAdmin(InetAddress failed) {
        //fires an email to the administrator using a webhook about the failed master
    }

    @Async
    public CompletableFuture<Void> sendToNodes(RedisNodes nodes, PutRequest req) {
        Objects.requireNonNull(nodes, "nodes must not be null");
        Objects.requireNonNull(req, "req must not be null");

        List<InetAddress> addrs = nodes.getAddresses();
        if (addrs.isEmpty()) return CompletableFuture.completedFuture(null);

        final String json;
        try {
            json = objectMapper.writeValueAsString(req);
        } catch (JsonProcessingException e) {
            log.warn("Failed to serialize PutRequest for sending to slaves", e);
            return CompletableFuture.failedFuture(e);
        }

        List<CompletableFuture<HttpResponse<Void>>> futures = new ArrayList<>();

        for (InetAddress addr : addrs) {
            if(!redisNodes.getMasterNodes().contains(addr)) {
                String host = addr.getHostAddress();
                String url = "http://" + host + ":8080/v1";
                HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .timeout(Duration.ofSeconds(5))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(json))
                        .build();

                CompletableFuture<HttpResponse<Void>> f = httpClient
                        .sendAsync(request, HttpResponse.BodyHandlers.discarding())
                        .whenComplete((resp, ex) -> {
                            if (ex != null) {
                                log.warn("Failed to send request to {}: {}", host, ex.getMessage());
                            } else if (resp.statusCode() >= 400) {
                                log.warn("Slave {} returned HTTP {}", host, resp.statusCode());
                            } else {
                                log.debug("Successfully sent update to {}", host);
                            }
                        });

                futures.add(f);
                }
        }

        CompletableFuture<?> all = CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
        return all.thenApply(v -> null);
    }

    @Async
    public CompletableFuture<Void> send_to_nodes(RedisNodes nodes, PutRequest req) {
        return sendToNodes(nodes, req);
    }
}
