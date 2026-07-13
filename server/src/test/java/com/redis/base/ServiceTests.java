package com.redis.base;

import com.redis.base.DTO.PutRequest;
import com.redis.base.Service.Service;
import com.redis.base.Storage.KVStore;
import com.redis.base.Service.SentinelService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class ServiceTests {

    @Mock
    private KVStore kvStore;

    @Mock
    private SentinelService sentinelService;

    private Service service;

    @BeforeEach
    void setUp() {
    service = new Service(kvStore, sentinelService);
        // By default, assume the key does not exist (no recorded type).
        // Use lenient stubbing so tests that exercise validation (which don't call
        // getKeyType) don't fail with unnecessary stubbing exceptions.
        lenient().when(kvStore.getKeyType(anyString())).thenReturn(null);
    }

    @Test
    void putString_shouldCallKVStorePutString_andReturnPrevious() {
        PutRequest req = new PutRequest("key1", "string", "hello");
        when(kvStore.putString(eq("key1"), eq("hello"))).thenReturn(null);

        Object prev = service.put(req);

        assertNull(prev);
        verify(kvStore, times(1)).putString("key1", "hello");
    }

    @Test
    void putInteger_fromString_shouldParseAndCallPutInteger() {
        PutRequest req = new PutRequest("kint", "int", "42");
        when(kvStore.putInteger(eq("kint"), eq(42))).thenReturn(0);

        Object prev = service.put(req);

        assertEquals(0, prev);
        verify(kvStore, times(1)).putInteger("kint", 42);
    }

    @Test
    void get_existingKey_shouldReturnMapWithValue() {
        when(kvStore.getKeyType("kx")).thenReturn("string");
        when(kvStore.getString("kx")).thenReturn("valx");

        Map<String, Object> res = service.get("kx");

        assertEquals("kx", res.get("key"));
        assertEquals("string", res.get("type"));
        assertEquals("valx", res.get("value"));
    }

    @Test
    void put_nullRequest_shouldThrow() {
        assertThrows(IllegalArgumentException.class, () -> service.put(null));
    }

    @Test
    void put_unknownType_shouldThrow() {
        PutRequest req = new PutRequest("k", "unknown", "v");
        assertThrows(IllegalArgumentException.class, () -> service.put(req));
    }

    @Test
    void put_list_whenPreviouslyString_shouldRemovePreviousAndPutList() {
    // simulate putList behavior; Service no longer queries getKeyType or
    // removes the previous typed entry — KVStore is responsible for moves.
    when(kvStore.putList(eq("mk"), anyList())).thenReturn(null);

        PutRequest req = new PutRequest("mk", "list", List.of("a", "b"));
        Object prev = service.put(req);
        assertNull(prev);

        // previous value was removed from string bucket and new list was put
    // Service should not call removeString (KVStore handles moves); it
    // should call putList to insert the new value.
    verify(kvStore, never()).removeString("mk");
    verify(kvStore, times(1)).putList(eq("mk"), anyList());
    }

    @Test
    void get_missingKey_shouldThrowNoSuchElement() {
        when(kvStore.getKeyType("missing")).thenReturn(null);
        assertThrows(NoSuchElementException.class, () -> service.get("missing"));
    }
}
