package com.redis.base;

// imports adjusted: Service and PutRequest are in same package, explicit import of Service not required
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.InetAddress;
import java.util.*;

import com.redis.base.DTO.PutRequest;
import com.redis.base.Service.Service;


@RestController
@RequestMapping("/v1")
public class Routes {

	private final Service service;

	@Autowired
	public Routes(Service service) {
		this.service = service;
	}

	/**
	 * Update the list of nodes. Expects a JSON array of hostname/IP strings.
	 * Example: PUT /v1/nodes ["10.0.0.2", "slave.example.com"]
	 */
	@PutMapping("/nodes")
	public ResponseEntity<?> updateNodes(@RequestBody List<String> addrs) {
		try {
			service.updateNodes(addrs);
			return ResponseEntity.ok(Map.of("updated", true));
		} catch (IllegalArgumentException iae) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", iae.getMessage()));
		} catch (Exception e) {
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", e.getMessage()));
		}
	}

	/**
	 * Toggle/Set whether this node is master. Example: PATCH /v1/isMaster/true
	 */
	@PatchMapping("/isMaster/{isMaster}")
	public ResponseEntity<?> updateIsMaster(@PathVariable boolean isMaster) {
		try {
			service.updateIsMaster(isMaster);
			return ResponseEntity.ok(Map.of("isMaster", isMaster));
		} catch (IllegalArgumentException iae) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", iae.getMessage()));
		} catch (Exception e) {
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", e.getMessage()));
		}
	}

	// PutRequest is now defined in com.redis.base.Service

	/**
	 * Put an object into the typed KVStore.
	 * Request JSON shape:
	 * {
	 *   "key": "myKey",
	 *   "valueType": "string|integer|list|set|map",
	 *   "value": <any JSON value>
	 * }
	 */
	@PutMapping
	public ResponseEntity<?> put(@RequestBody PutRequest req) {


		try {
			Object prev = service.put(req);
			return ResponseEntity.ok(Map.of("previous", prev));
		} catch (NumberFormatException nfe) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", "invalid number format: " + nfe.getMessage()));
		} catch (IllegalArgumentException iae) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", iae.getMessage()));
		} catch (Exception e) {
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", e.getMessage()));
		}
	}

	/**
	 * Retrieve a value by key. The overall key->type map is consulted first to
	 * determine which typed-bucket to read from. Example: GET /v1/key/myKey
	 */
	@GetMapping("/key/{key}")
	public ResponseEntity<?> get(@PathVariable String key) {
		if (key == null) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", "key is required"));
		}



		try {
			Map<String, Object> result = service.get(key);
			return ResponseEntity.ok(result);
		} catch (IllegalArgumentException iae) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", iae.getMessage()));
		} catch (NoSuchElementException nse) {
			return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", nse.getMessage()));
		} catch (IllegalStateException ise) {
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", ise.getMessage()));
		}
	}

	@GetMapping("/healthcheck")
	public ResponseEntity<?> healthCheck() {
		// Simple liveness endpoint that returns 200 OK
		return ResponseEntity.ok().build();
	}

	@GetMapping("/checkIfMaster")
	public ResponseEntity<?> checkIfMaster() {
		// Return the master flag from the service layer as a raw boolean
		boolean isMaster = service.checkIfMaster();
		return ResponseEntity.ok(isMaster);
	}

	@PostMapping("/registerClient")
	public ResponseEntity<?> registerClient(@RequestBody Map<String, Object> body) {
		try {
			String address = body.get("address") == null ? null : String.valueOf(body.get("address"));
			Object portObj = body.get("port");
			if (address == null || address.isBlank() || portObj == null) {
				throw new IllegalArgumentException("address and port are required");
			}
			int port;
			if (portObj instanceof Number) {
				port = ((Number) portObj).intValue();
			} else {
				port = Integer.parseInt(String.valueOf(portObj));
			}

			String combined = address.trim() + ":" + port;
			List<Map<String, String>> res = service.registerClient(combined);
			return ResponseEntity.ok(res);
		} catch (NumberFormatException nfe) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", "invalid port"));
		} catch (IllegalArgumentException iae) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", iae.getMessage()));
		} catch (IllegalStateException ise) {
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", ise.getMessage()));
		}
	}

}
