package com.redis.base.Storage;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class KVStoreTests {

    @Autowired
    private KVStore store;

    @Test
    void springContextInjectsBean() {
        assertNotNull(store, "KVStore should be injected by Spring");
    }

    @Test
    void putGetRemove() {
    store.clearAll();
        assertNull(store.putString("k", "v"));
        assertEquals("v", store.getString("k"));
        assertEquals("v", store.removeString("k"));
        assertNull(store.getString("k"));
    }

    @Test
    void concurrentPutGet() throws Exception {
    store.clearAll();

        int threads = 8;
        int per = 1000;
        ExecutorService ex = Executors.newFixedThreadPool(threads);
        CountDownLatch latch = new CountDownLatch(threads);

        for (int i = 0; i < threads; i++) {
            final int id = i;
            ex.submit(() -> {
                for (int j = 0; j < per; j++) {
                    store.putInteger(id + "-" + j, j);
                }
                latch.countDown();
            });
        }

        latch.await();
        ex.shutdown();
        assertEquals(threads * per, store.sizeIntegers());
    }
}
