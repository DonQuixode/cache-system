package com.redis.base.Storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.net.InetAddress;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = RedisNodes.class)
public class RedisNodesTests {

    @Autowired
    RedisNodes nodes;

    @BeforeEach
    void clear() {
        nodes.setAddresses(List.of());
    }

    @Test
    void add_and_getAddresses_and_preventDuplicate() throws Exception {
        InetAddress a = InetAddress.getByName("127.0.0.1");
        boolean added = nodes.add(a);
        assertTrue(added);

        List<InetAddress> list = nodes.getAddresses();
        assertEquals(1, list.size());
        assertEquals(a, list.get(0));

        // adding again should not add duplicate
        boolean addedAgain = nodes.add(a);
        assertFalse(addedAgain);
        assertEquals(1, nodes.getAddresses().size());
    }

    @Test
    void remove_existing_and_nonExisting() throws Exception {
        InetAddress a = InetAddress.getByName("127.0.0.1");
        InetAddress b = InetAddress.getByName("127.0.0.2");
        nodes.add(a);
        nodes.add(b);
        assertEquals(2, nodes.getAddresses().size());

        boolean removed = nodes.remove(a);
        assertTrue(removed);
        assertEquals(1, nodes.getAddresses().size());
        assertFalse(nodes.getAddressSet().contains(a));

        // removing again should return false
        assertFalse(nodes.remove(a));
    }

    @Test
    void setAddresses_replaces_existing() throws Exception {
        InetAddress a = InetAddress.getByName("127.0.0.1");
        InetAddress b = InetAddress.getByName("127.0.0.2");
        nodes.add(a);
        assertEquals(1, nodes.getAddresses().size());

        nodes.setAddresses(List.of(b, a));
        List<InetAddress> list = nodes.getAddresses();
        assertEquals(2, list.size());
        Set<InetAddress> set = nodes.getAddressSet();
        assertEquals(2, set.size());
    }
}
