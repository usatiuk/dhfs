package com.usatiuk.objects.alloc.it;

import com.usatiuk.objects.alloc.runtime.ObjectAllocator;
import com.usatiuk.objects.common.JObjectKey;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class ObjectsAllocResourceTest {
    @Inject
    ObjectAllocator objectAllocator;

    @Test
    void testCreateObject() {
        var newObject = objectAllocator.create(TestJDataEmpty.class, new JObjectKey("TestJDataEmptyKey"));
        Assertions.assertNotNull(newObject);
        Assertions.assertEquals("TestJDataEmptyKey", newObject.getKey().name());
    }
}
