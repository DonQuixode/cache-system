package com.redis.base.Storage;

import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Spring-managed singleton container for Redis node addresses.
 *
 * - The address list is a CopyOnWriteArrayList for safe and quick iteration. Its why Set isn't used for iteration.
 * - The address set is a concurrent key-set (backed by ConcurrentHashMap) used
 *   to quickly check membership and avoid duplicates.
 */
@Component
public final class RedisNodes {

    private final CopyOnWriteArrayList<InetAddress> addresses = new CopyOnWriteArrayList<>();
    private final Set<InetAddress> hashOfAddresses = ConcurrentHashMap.newKeySet();
    // Track master nodes separately. Use a concurrent key-set for thread-safety.
    private final Set<InetAddress> masterNodes = ConcurrentHashMap.newKeySet();

    public RedisNodes() { }

    /**
     * Get an immutable snapshot of the addresses list.
     */
    public List<InetAddress> getAddresses() {
        return Collections.unmodifiableList(new ArrayList<>(addresses));
    }

    /**
     * Initialize/replace all addresses from a list. Uses the single-address add
     * function for each element to preserve the membership semantics.
     */
    public synchronized void setAddresses(List<InetAddress> addrs) {
        addresses.clear();
        hashOfAddresses.clear();
        if (addrs == null) return;
        for (InetAddress a : addrs) {
            add(a);
        }
    }

    /**
     * Initialize/replace with a single address.
     */
    public synchronized void setAddress(InetAddress addr) {
        addresses.clear();
        hashOfAddresses.clear();
        if (addr != null) add(addr);
    }

    /**
     * Add a single address. Returns true if added, false if it was already present.
     * The list-add delegates to this method when adding multiple addresses.
     */

    public synchronized void add(List<InetAddress> addrs) {
        Objects.requireNonNull(addrs, "addr must not be null");
        for(InetAddress addr: addrs) {
            add(addr);
        }
    }

    public boolean add(InetAddress addr) {
        Objects.requireNonNull(addr, "addr must not be null");
        // Check and add to set first to prevent duplicates in list
        boolean added = hashOfAddresses.add(addr);
        if (!added) return false;
        addresses.add(addr);
        // Newly added nodes are not masters by default; ensure master set is updated accordingly
        isMasterNode(addr);
        return true;
    }

    /**
     * Remove a single address. Returns true if removed, false if it was not present.
     */
    public boolean remove(InetAddress addr) {
        Objects.requireNonNull(addr, "addr must not be null");
        boolean present = hashOfAddresses.remove(addr);
        if (!present) return false;
        addresses.remove(addr);
        return true;
    }

    /**
     * Set or clear master status for a single address.
     * If isMaster is true the address will be added to masterNodes; if false
     * it will be removed from masterNodes. Returns true when the call made a
     * change (add or remove), false otherwise.
     */
    public boolean isMasterNode(InetAddress addr) {
        Objects.requireNonNull(addr, "addr must not be null");

        // Query the remote node's checkIfMaster endpoint and update local master set
        String host = addr.getHostAddress();
        String url = "http://" + host + ":8080/v1/checkIfMaster";

        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(2))
                .build();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(3))
                .GET()
                .build();

        try {
            HttpResponse<String> resp = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() / 100 != 2) {
                // Non-2xx treated as not master
                masterNodes.remove(addr);
                return false;
            }
            String body = resp.body();
            boolean remoteIsMaster = Boolean.parseBoolean(body == null ? "false" : body.trim());
            if (remoteIsMaster) {
                masterNodes.add(addr);
            } else {
                masterNodes.remove(addr);
            }
            return remoteIsMaster;
        } catch (Exception e) {
            // On any error (IO, timeout, etc) conservatively treat as not master
            masterNodes.remove(addr);
            return false;
        }
    }

    /**
     * Snapshot view of the address set (read-only).
     */
    public Set<InetAddress> getAddressSet() {
        return Collections.unmodifiableSet(hashOfAddresses);
    }

    /**
     * Return a snapshot of the configured master nodes.
     */
    public Set<InetAddress> getMasterNodes() {
        // Return an immutable snapshot to prevent callers from mutating the internal set
        return Collections.unmodifiableSet(new HashSet<>(masterNodes));
    }

    /**
     * Add or update a single master node. Returns true if the node was added,
     * false if it was already present.
     */
    public boolean updateMasterNode(InetAddress addr) {
        Objects.requireNonNull(addr, "addr must not be null");
        return masterNodes.add(addr);
    }

    /**
     * Remove a single master node. Returns true if the node was removed,
     * false if it was not present.
     */
    public boolean removeMasterNode(InetAddress addr) {
        Objects.requireNonNull(addr, "addr must not be null");
        return masterNodes.remove(addr);
    }

    /**
     * Poll all configured addresses and refresh their master status by
     * calling each node's /v1/checkIfMaster endpoint via isMasterNode.
     * The method updates the local masterNodes set accordingly (add or remove).
     */
    public void updateMaster() {
        List<InetAddress> addrs = getAddresses();
        for (InetAddress addr : addrs) {
            boolean remoteMaster = isMasterNode(addr);
            if (remoteMaster) {
                masterNodes.add(addr);
            } else {
                masterNodes.remove(addr);
            }
        }
    }
}
