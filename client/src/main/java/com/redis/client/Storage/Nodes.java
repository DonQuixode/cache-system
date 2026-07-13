package com.redis.client.Storage;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Objects;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

@Component
public class Nodes {

	private final List<InetAddress> masters = Collections.synchronizedList(new ArrayList<>());
	private final List<InetAddress> slaves = Collections.synchronizedList(new ArrayList<>());

	// lock to make reconfiguration as atomic as possible
	private final Object reconfigureLock = new Object();

	private final ObjectMapper mapper = new ObjectMapper();

	// Construct with single address -> fetch all nodes from API and populate
	public Nodes(String seedAddress) {
		try {
			InetAddress seed = InetAddress.getByName(seedAddress);
			// store seed temporarily (could be used) then fetch from API
			fetchAndPopulate(seed);
		} catch (UnknownHostException e) {
			throw new IllegalArgumentException("Invalid seed address: " + seedAddress, e);
		}
	}

	// Construct with two addresses: first is slave, second is master
	public Nodes(List<InetAddress> slaves, List<InetAddress> masters) {
		if (slaves.size() == 0 || masters.size() == 0) {
			throw new IllegalArgumentException("cannot initialize. Empty slave and master");
		}
		try {
            fetchAndPopulate(slaves.get(0));
			for(InetAddress addr: slaves) {
                this.slaves.add(addr);
                queryNodesApi(addr);
            }
            for(InetAddress addr: masters) {
                this.masters.add(addr);
                queryNodesApi(addr);
            }
		} catch (Exception e) {
			throw new IllegalArgumentException("Invalid address in list", e);
		}
	}


	public List<InetAddress> getMasters() {
		return Collections.unmodifiableList(masters);
	}

	public List<InetAddress> getSlaves() {
		return Collections.unmodifiableList(slaves);
	}

	// Hash a key to an index in [0, list.size()-1].
	// Uses the key's hashCode with unsigned conversion to ensure non-negative result.
	public static <T> int hashToIndex(Object key, List<T> list) {
		if (list == null || list.isEmpty()) {
			throw new IllegalArgumentException("List must not be null or empty");
		}
		// allow null key: treat as 0
		int h = Objects.hashCode(key);
		return (h & 0x7fffffff) % list.size();
	}

	// The remote endpoint is expected to return a JSON map of address -> type
	// e.g. {"10.0.0.5":"master","10.0.0.6":"slave"}
	private void fetchAndPopulate(InetAddress seed) {
		Map<String, String> nodes = queryNodesApi(seed);
		if (nodes == null || nodes.isEmpty()) return;
		for (Map.Entry<String, String> e : nodes.entrySet()) {
			String address = e.getKey();
			String role = e.getValue();
			if (address == null || role == null) continue;
			try {
				InetAddress ia = InetAddress.getByName(address);
				if ("master".equalsIgnoreCase(role)) {
					masters.add(ia);
				} else if ("slave".equalsIgnoreCase(role)) {
					slaves.add(ia);
				}
			} catch (UnknownHostException ex) {
				// skip invalid address
			}
		}
	}

	// Sends a PUT to http://{seedHost}:8080/v1/registerClient with body {"address":"<local-ip>"}
	// and parses the JSON map response into a Map<String,String> (address -> role).
	private Map<String, String> queryNodesApi(InetAddress seed) {
		try {
			String seedHost = seed.getHostAddress();
			String url = "http://" + seedHost + ":8080/v1/registerClient";

			RestTemplate rt = new RestTemplate();

			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_JSON);

			String localAddress = InetAddress.getLocalHost().getHostAddress();
            String port = Integer.toString(8080);
            Map<String, String> JSONMap = new HashMap<>();
            JSONMap.put("address", localAddress);
            JSONMap.put("port", port);
			String body = mapper.writeValueAsString(JSONMap);

			HttpEntity<String> entity = new HttpEntity<>(body, headers);

			ResponseEntity<String> resp = rt.exchange(url, HttpMethod.PUT, entity, String.class);
			if (resp.getStatusCode().is2xxSuccessful() && resp.getBody() != null) {
				// parse JSON into map
				return mapper.readValue(resp.getBody(), mapper.getTypeFactory().constructMapType(Map.class, String.class, String.class));
			}

            
		} catch (Exception e) {
			// ignore - return empty map on any failure
		}
		return Collections.emptyMap();
	}

	// Select a slave using hash and send GET /v1/key/{key}
	public String getCall(String key) {
		if (key == null) throw new IllegalArgumentException("key must not be null");
		if (slaves.isEmpty()) throw new IllegalStateException("No slave nodes available");
		int idx = hashToIndex(key, slaves);
		InetAddress node = slaves.get(idx);
		try {
			String encoded = URLEncoder.encode(key, StandardCharsets.UTF_8.toString());
			String url = "http://" + node.getHostAddress() + ":8080/v1/key/" + encoded;
			RestTemplate rt = new RestTemplate();
			ResponseEntity<String> resp = rt.getForEntity(url, String.class);
			return resp.getBody();
		} catch (Exception e) {
			throw new RuntimeException("GET call failed", e);
		}
	}

	// Select a master using hash and send POST /v1 with JSON body {key, valueType, value}
	public String putCall(String key, String valueType, Object value) {
		if (key == null) throw new IllegalArgumentException("key must not be null");
		if (valueType == null) throw new IllegalArgumentException("valueType must not be null");
		if (masters.isEmpty()) throw new IllegalStateException("No master nodes available");
		int idx = hashToIndex(key, masters);
		InetAddress node = masters.get(idx);
		try {
			String url = "http://" + node.getHostAddress() + ":8080/v1";

			RestTemplate rt = new RestTemplate();
			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_JSON);

			Map<String, Object> bodyMap = new java.util.HashMap<>();
			bodyMap.put("key", key);
			bodyMap.put("valueType", valueType);
			bodyMap.put("value", value);

			String body = mapper.writeValueAsString(bodyMap);
			HttpEntity<String> entity = new HttpEntity<>(body, headers);

			ResponseEntity<String> resp = rt.postForEntity(url, entity, String.class);
			return resp.getBody();
		} catch (Exception e) {
			throw new RuntimeException("PUT call failed", e);
		}
	}

	/**
	 * Reconfigure masters and slaves from a provided map.
	 * The map's key is an InetAddress and value is "master" or "slave".
	 * This clears both lists first and repopulates them inside a single synchronized block.
	 */
	public void reconfigureNodes(Map<InetAddress, String> nodes) {
		if (nodes == null) throw new IllegalArgumentException("nodes map must not be null");
		synchronized (reconfigureLock) {
			masters.clear();
			slaves.clear();
			for (Map.Entry<InetAddress, String> e : nodes.entrySet()) {
				InetAddress addr = e.getKey();
				String role = e.getValue();
				if (addr == null || role == null) continue;
				if ("master".equalsIgnoreCase(role)) {
					masters.add(addr);
				} else if ("slave".equalsIgnoreCase(role)) {
					slaves.add(addr);
				}
			}
		}
	}

}
