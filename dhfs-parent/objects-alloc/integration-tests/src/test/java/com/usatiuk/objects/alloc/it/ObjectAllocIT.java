package com.usatiuk.objects.alloc.it;

import com.usatiuk.objects.alloc.runtime.ObjectAllocator;
import com.usatiuk.objects.common.runtime.JObjectKey;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@QuarkusTest
public class ObjectAllocIT {
    @Inject
    ObjectAllocator objectAllocator;

    @Test
    void testCreateObject() {
        var newObject = objectAllocator.create(TestJDataEmpty.class, new JObjectKey("TestJDataEmptyKey"));
        Assertions.assertNotNull(newObject);
        Assertions.assertEquals("TestJDataEmptyKey", newObject.getKey().name());
    }


    @Test
    void testCopyObject() {
        var newObject = objectAllocator.create(TestJDataAssorted.class, new JObjectKey("TestJDataAssorted"));
        newObject.setLastName("1");
        Assertions.assertNotNull(newObject);
        Assertions.assertEquals("TestJDataAssorted", newObject.getKey().name());

        var copyObject = objectAllocator.copy(newObject);
        Assertions.assertNotNull(copyObject);
        Assertions.assertFalse(copyObject.isModified());
        Assertions.assertEquals("1", copyObject.wrapped().getLastName());
        copyObject.wrapped().setLastName("2");
        Assertions.assertTrue(copyObject.isModified());
        Assertions.assertEquals("2", copyObject.wrapped().getLastName());
        Assertions.assertEquals("1", newObject.getLastName());
    }

    @Test
    void testImmutable() {
        var newObject = objectAllocator.create(TestJDataAssorted.class, new JObjectKey("TestJDataAssorted"));
        newObject.setLastName("1");
        Assertions.assertNotNull(newObject);
        Assertions.assertEquals("TestJDataAssorted", newObject.getKey().name());

        var copyObject = objectAllocator.unmodifiable(newObject);
        Assertions.assertNotNull(copyObject);
        Assertions.assertEquals("1", copyObject.getLastName());
        Assertions.assertThrows(UnsupportedOperationException.class, () -> copyObject.setLastName("2"));
    }

    @Test
    void testImmutable2() {
        var newObject = objectAllocator.create(TestJDataEmpty.class, new JObjectKey("TestJDataEmpty"));
        Assertions.assertNotNull(newObject);
        Assertions.assertEquals("TestJDataEmpty", newObject.getKey().name());

        var copyObject = objectAllocator.unmodifiable(newObject);
        Assertions.assertNotNull(copyObject);
    }
}
