package com.usatiuk.autoprotomap.it;

import com.usatiuk.autoprotomap.runtime.ProtoSerializer;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


@QuarkusTest
public class AutoprotomapResourceTest {
    @Inject
    ProtoSerializer<SimpleObjectProto, SimpleObject> protoSerializer;
    @Inject
    ProtoSerializer<NestedObjectProto, NestedObject> nestedProtoSerializer;

    @Test
    public void testSimple() {
        var ret = protoSerializer.serialize(new SimpleObject(1234));
        Assertions.assertEquals(1234, ret.getNumfield());
    }

    @Test
    public void testNested() {
        var ret = nestedProtoSerializer.serialize(new NestedObject(new SimpleObject(333)));
        Assertions.assertEquals(333, ret.getObject().getNumfield());
    }
}
