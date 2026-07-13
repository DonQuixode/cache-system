package com.redis.base.Client;

import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class RedisClients {

	// stores client IP -> port
	private final ConcurrentHashMap<InetAddress, Integer> clients = new ConcurrentHashMap<>();

	public RedisClients() {
	}

	/**
	 * Check whether the given InetAddress is present in the clients map.
	 */
	public boolean isPresent(InetAddress addr) {
		Objects.requireNonNull(addr, "addr must not be null");
		return clients.containsKey(addr);
	}

	/**
	 * Return the clients as a list of webhook URL strings of the form:
	 * <host>:<port>/redis/webhook
	 */
	public List<String> getClients() {
		List<String> out = new ArrayList<>();
		for (Map.Entry<InetAddress, Integer> e : clients.entrySet()) {
			InetAddress addr = e.getKey();
			Integer port = e.getValue();
			String host = addr.getHostAddress();
			out.add(host + ":" + port + "/redis/webhook");
		}
		return out;
	}

	/**
	 * Put multiple clients into the map. Each InetSocketAddress is passed to putClient.
	 */
	public void putClients(List<InetSocketAddress> addrs) {
		Objects.requireNonNull(addrs, "addrs must not be null");
		for (InetSocketAddress isa : addrs) {
			putClient(isa);
		}
	}

	/**
	 * Put a single client into the map. Extracts InetAddress and port from the
	 * supplied InetSocketAddress. If the address is unresolved, attempts to resolve
	 * by host name.
	 */
	public void putClient(InetSocketAddress isa) {
		Objects.requireNonNull(isa, "isa must not be null");

		InetAddress addr = isa.getAddress();
		if (addr == null) {
			// unresolved - try to resolve
			String host = isa.getHostString();
			try {
				addr = InetAddress.getByName(host);
			} catch (UnknownHostException e) {
				throw new IllegalArgumentException("Unable to resolve host: " + host, e);
			}
		}

		int port = isa.getPort();
		// put or update existing entry
		clients.put(addr, port);
	}

	// Expose internal map for advanced callers (if needed). Returns the live map.
	public ConcurrentHashMap<InetAddress, Integer> getClientsMap() {
		return clients;
	}
}
