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
    void editObject() {
        {
            txm.begin();
            var newParent = alloc.create(Parent.class, new JObjectKey("Parent3"));
            newParent.setLastName("John");
            curTx.putObject(newParent);
            txm.commit();
        }

        {
            txm.begin();
            var parent = curTx.getObject(Parent.class, new JObjectKey("Parent3"), LockingStrategy.OPTIMISTIC).orElse(null);
            Assertions.assertEquals("John", parent.getLastName());
            parent.setLastName("John2");
            txm.commit();
        }

        {
            txm.begin();
            var parent = curTx.getObject(Parent.class, new JObjectKey("Parent3"), LockingStrategy.WRITE).orElse(null);
            Assertions.assertEquals("John2", parent.getLastName());
            parent.setLastName("John3");
            txm.commit();
        }

        {
            txm.begin();
            var parent = curTx.getObject(Parent.class, new JObjectKey("Parent3"), LockingStrategy.READ_ONLY).orElse(null);
            Assertions.assertEquals("John3", parent.getLastName());
            txm.commit();
        }
    }

    @Test
    void createObjectConflict() throws InterruptedException {
        AtomicBoolean thread1Failed = new AtomicBoolean(true);
        AtomicBoolean thread2Failed = new AtomicBoolean(true);

        var signal = new Semaphore(0);
        var signalFin = new Semaphore(2);


        Just.run(() -> {
            try {
                signalFin.acquire();
                Log.warn("Thread 1");
                txm.begin();
                var newParent = alloc.create(Parent.class, new JObjectKey("Parent2"));
                newParent.setLastName("John");
                curTx.putObject(newParent);
                signal.acquire();
                Log.warn("Thread 1 commit");
                txm.commit();
                thread1Failed.set(false);
                signal.release();
                return null;
            } finally {
                signalFin.release();
            }
        });
        Just.run(() -> {
            try {
                signalFin.acquire();
                Log.warn("Thread 2");
                txm.begin();
                var newParent = alloc.create(Parent.class, new JObjectKey("Parent2"));
                newParent.setLastName("John2");
                curTx.putObject(newParent);
                signal.acquire();
                Log.warn("Thread 2 commit");
                txm.commit();
                thread2Failed.set(false);
                signal.release();
                return null;
            } finally {
                signalFin.release();
            }
        });

        signal.release(2);
        signalFin.acquire(2);

        txm.begin();
        var got = curTx.getObject(Parent.class, new JObjectKey("Parent2"), LockingStrategy.READ_ONLY).orElse(null);
        txm.commit();

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
