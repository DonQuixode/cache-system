package com.redis.client.Controller;

import java.net.InetAddress;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.redis.client.Storage.Nodes;

@RestController
@RequestMapping("/redis")
public class Routes {
    
    private final Nodes nodes;

    @Autowired
    public Routes(Nodes nodes) {
        this.nodes = nodes;
    }

    @PostMapping("/webhook")
    public ResponseEntity<?> post(@RequestBody Map<InetAddress,String> body) {
        try {
            nodes.reconfigureNodes(body);
            return ResponseEntity.ok("Accepted");
        } catch (IllegalArgumentException iae) {
			return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("error", iae.getMessage()));
		} catch (IllegalStateException ise) {
			return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", ise.getMessage()));
		}
    }
    
}
