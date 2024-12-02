package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.data.Parent;
import com.usatiuk.dhfs.objects.transaction.LockingStrategy;
import io.quarkus.logging.Log;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

@QuarkusTest
public class ObjectsTest {
    @Inject
    TransactionManager txm;

    @Inject
    CurrentTransaction curTx;

    @Inject
    ObjectAllocator alloc;

    @Test
    void createObject() {
        {
            txm.begin();
            var newParent = alloc.create(Parent.class, new JObjectKey("Parent"));
            newParent.setLastName("John");
            curTx.putObject(newParent);
            txm.commit();
        }

        {
            txm.begin();
            var parent = curTx.getObject(Parent.class, new JObjectKey("Parent"), LockingStrategy.READ_ONLY).orElse(null);
            Assertions.assertEquals("John", parent.getLastName());
            txm.commit();
        }
    }

    @Test
    void createObjectConflict() throws InterruptedException {
        AtomicBoolean thread1Failed = new AtomicBoolean(true);
        AtomicBoolean thread2Failed = new AtomicBoolean(true);

        var signal = new Semaphore(0);

        new Thread(() -> {
            Log.warn("Thread 1");
            txm.begin();
            var newParent = alloc.create(Parent.class, new JObjectKey("Parent2"));
            newParent.setLastName("John");
            curTx.putObject(newParent);
            try {
                signal.acquire();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            Log.warn("Thread 1 commit");
            txm.commit();
            thread1Failed.set(false);
        }).start();

        new Thread(() -> {
            Log.warn("Thread 2");
            txm.begin();
            var newParent = alloc.create(Parent.class, new JObjectKey("Parent2"));
            newParent.setLastName("John2");
            curTx.putObject(newParent);
            try {
                signal.acquire();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            Log.warn("Thread 2 commit");
            txm.commit();
            thread2Failed.set(false);
        }).start();

        signal.release(2);

        Thread.sleep(500);

        txm.begin();
        var got = curTx.getObject(Parent.class, new JObjectKey("Parent2"), LockingStrategy.READ_ONLY).orElse(null);

        if (!thread1Failed.get()) {
            Assertions.assertTrue(thread2Failed.get());
            Assertions.assertEquals("John", got.getLastName());
        } else if (!thread2Failed.get()) {
            Assertions.assertEquals("John2", got.getLastName());
        } else {
            Assertions.fail("No thread succeeded");
        }
    }

//    @Test
//    void editConflict() {
//        {
//            var tx = _tx.beginTransaction();
//            var parent = tx.getObject(new JObjectKey("Parent"), Parent.class);
//            parent.setName("John");
//            tx.commit();
//        }
//
//        {
//            var tx = _tx.beginTransaction();
//            var parent = tx.getObject(new JObjectKey("Parent"), Parent.class);
//            parent.setName("John2");
//
//            var tx2 = _tx.beginTransaction();
//            var parent2 = tx2.getObject(new JObjectKey("Parent"), Parent.class);
//            parent2.setName("John3");
//
//            tx.commit();
//            Assertions.assertThrows(Exception.class, tx2::commit);
//        }
//
//        {
//            var tx2 = _tx.beginTransaction();
//            var parent = tx2.getObject(new JObjectKey("Parent"));
//            Assertions.assertInstanceOf(Parent.class, parent);
//            Assertions.assertEquals("John2", ((Parent) parent).getName());
//        }
//    }
//
//    @Test
//    void nestedCreate() {
//        {
//            var tx = _tx.beginTransaction();
//            var parent = tx.getObject(new JObjectKey("Parent"), Parent.class);
//            var kid = tx.getObject(new JObjectKey("Kid"), Kid.class);
//            parent.setName("John");
//            kid.setName("KidName");
//            parent.setKidKey(kid.getKey());
//            tx.commit();
//        }
//
//        {
//            var tx2 = _tx.beginTransaction();
//            var parent = tx2.getObject(new JObjectKey("Parent"));
//            Assertions.assertInstanceOf(Parent.class, parent);
//            Assertions.assertEquals("John", ((Parent) parent).getName());
//            Assertions.assertEquals("KidName", ((Parent) parent).getKid().getName());
//        }
//    }

}
