package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.persistence.FakeObjectStorage;
import com.usatiuk.dhfs.objects.test.objs.Kid;
import com.usatiuk.dhfs.objects.test.objs.Parent;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ObjectsTest {
    private final FakeObjectStorage _storage = new FakeObjectStorage();
    private final JObjectManager _tx = new JObjectManager(_storage);

    @Test
    void createObject() {
        {
            var tx = _tx.beginTransaction();
            var parent = tx.getObject(new JObjectKey("Parent"), Parent.class);
            parent.setName("John");
            tx.commit();
        }

        {
            var tx2 = _tx.beginTransaction();
            var parent = tx2.getObject(new JObjectKey("Parent"));
            Assertions.assertInstanceOf(Parent.class, parent);
            Assertions.assertEquals("John", ((Parent) parent).getName());
        }
    }

    @Test
    void createObjectConflict() {
        {
            var tx = _tx.beginTransaction();
            var parent = tx.getObject(new JObjectKey("Parent"), Parent.class);
            parent.setName("John");

            var tx2 = _tx.beginTransaction();
            var parent2 = tx2.getObject(new JObjectKey("Parent"), Parent.class);
            parent2.setName("John");

            tx.commit();
            Assertions.assertThrows(Exception.class, tx2::commit);
        }
    }

    @Test
    void editConflict() {
        {
            var tx = _tx.beginTransaction();
            var parent = tx.getObject(new JObjectKey("Parent"), Parent.class);
            parent.setName("John");
            tx.commit();
        }

        {
            var tx = _tx.beginTransaction();
            var parent = tx.getObject(new JObjectKey("Parent"), Parent.class);
            parent.setName("John2");

            var tx2 = _tx.beginTransaction();
            var parent2 = tx2.getObject(new JObjectKey("Parent"), Parent.class);
            parent2.setName("John3");

            tx.commit();
            Assertions.assertThrows(Exception.class, tx2::commit);
        }

        {
            var tx2 = _tx.beginTransaction();
            var parent = tx2.getObject(new JObjectKey("Parent"));
            Assertions.assertInstanceOf(Parent.class, parent);
            Assertions.assertEquals("John2", ((Parent) parent).getName());
        }
    }

    @Test
    void nestedCreate() {
        {
            var tx = _tx.beginTransaction();
            var parent = tx.getObject(new JObjectKey("Parent"), Parent.class);
            var kid = tx.getObject(new JObjectKey("Kid"), Kid.class);
            parent.setName("John");
            kid.setName("KidName");
            parent.setKidKey(kid.getKey());
            tx.commit();
        }

        {
            var tx2 = _tx.beginTransaction();
            var parent = tx2.getObject(new JObjectKey("Parent"));
            Assertions.assertInstanceOf(Parent.class, parent);
            Assertions.assertEquals("John", ((Parent) parent).getName());
            Assertions.assertEquals("KidName", ((Parent) parent).getKid().getName());
        }
    }

}
