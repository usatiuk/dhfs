package com.usatiuk.autoprotomap.it;

import com.google.protobuf.ByteString;
import com.usatiuk.autoprotomap.runtime.ProtoSerializer;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


@QuarkusTest
public class AutoprotomapResourceTest {
    @Inject
    ProtoSerializer<SimpleObjectProto, SimpleObject> simpleProtoSerializer;
    @Inject
    ProtoSerializer<NestedObjectProto, NestedObject> nestedProtoSerializer;
    @Inject
    ProtoSerializer<AbstractProto, AbstractObject> abstractProtoSerializer;

    @Test
    public void testSimple() {
        var ret = simpleProtoSerializer.serialize(new SimpleObject(1234, "simple test", ByteString.copyFrom(new byte[]{1, 2, 3})));
        Assertions.assertEquals(1234, ret.getNumfield());
        Assertions.assertEquals("simple test", ret.getName());
        Assertions.assertEquals(ByteString.copyFrom(new byte[]{1, 2, 3}), ret.getSomeBytes());

        var des = simpleProtoSerializer.deserialize(ret);
        Assertions.assertEquals(1234, des.getNumfield());
        Assertions.assertEquals("simple test", des.getName());
        Assertions.assertEquals(ByteString.copyFrom(new byte[]{1, 2, 3}), des.getSomeBytes());
    }

    @Test
    public void testNested() {
        var ret = nestedProtoSerializer.serialize(
                new NestedObject(
                        new SimpleObject(333, "nested so", ByteString.copyFrom(new byte[]{1, 2, 3})),
                        "nested obj", ByteString.copyFrom(new byte[]{4, 5, 6})));
        Assertions.assertEquals(333, ret.getObject().getNumfield());
        Assertions.assertEquals("nested so", ret.getObject().getName());
        Assertions.assertEquals(ByteString.copyFrom(new byte[]{1, 2, 3}), ret.getObject().getSomeBytes());
        Assertions.assertEquals("nested obj", ret.getNestedName());
        Assertions.assertEquals(ByteString.copyFrom(new byte[]{4, 5, 6}), ret.getNestedSomeBytes());

        var des = nestedProtoSerializer.deserialize(ret);
        Assertions.assertEquals(333, des.object.numfield);
        Assertions.assertEquals(333, des.getObject().getNumfield());
        Assertions.assertEquals("nested so", des.getObject().getName());
        Assertions.assertEquals(ByteString.copyFrom(new byte[]{1, 2, 3}), des.getObject().getSomeBytes());
        Assertions.assertEquals("nested obj", des.get_nestedName());
        Assertions.assertEquals(ByteString.copyFrom(new byte[]{4, 5, 6}), des.get_nestedSomeBytes());
    }

    @Test
    public void testAbstractSimple() {
        var ret = abstractProtoSerializer.serialize(new SimpleObject(1234, "simple test", ByteString.copyFrom(new byte[]{1, 2, 3})));
        Assertions.assertEquals(1234, ret.getSimpleObject().getNumfield());
        Assertions.assertEquals("simple test", ret.getSimpleObject().getName());
        Assertions.assertEquals(ByteString.copyFrom(new byte[]{1, 2, 3}), ret.getSimpleObject().getSomeBytes());

        var des = (SimpleObject) abstractProtoSerializer.deserialize(ret);
        Assertions.assertEquals(1234, des.getNumfield());
        Assertions.assertEquals("simple test", des.getName());
        Assertions.assertEquals(ByteString.copyFrom(new byte[]{1, 2, 3}), des.getSomeBytes());
    }

    @Test
    public void tesAbstractNested() {
        var ret = abstractProtoSerializer.serialize(
                new NestedObject(
                        new SimpleObject(333, "nested so", ByteString.copyFrom(new byte[]{1, 2, 3})),
                        "nested obj", ByteString.copyFrom(new byte[]{4, 5, 6})));
        Assertions.assertEquals(333, ret.getNestedObject().getObject().getNumfield());
        Assertions.assertEquals("nested so", ret.getNestedObject().getObject().getName());
        Assertions.assertEquals(ByteString.copyFrom(new byte[]{1, 2, 3}), ret.getNestedObject().getObject().getSomeBytes());
        Assertions.assertEquals("nested obj", ret.getNestedObject().getNestedName());
        Assertions.assertEquals(ByteString.copyFrom(new byte[]{4, 5, 6}), ret.getNestedObject().getNestedSomeBytes());

        var des = (NestedObject) abstractProtoSerializer.deserialize(ret);
        Assertions.assertEquals(333, des.object.numfield);
        Assertions.assertEquals(333, des.getObject().getNumfield());
        Assertions.assertEquals("nested so", des.getObject().getName());
        Assertions.assertEquals(ByteString.copyFrom(new byte[]{1, 2, 3}), des.getObject().getSomeBytes());
        Assertions.assertEquals("nested obj", des.get_nestedName());
        Assertions.assertEquals(ByteString.copyFrom(new byte[]{4, 5, 6}), des.get_nestedSomeBytes());
    }
}
