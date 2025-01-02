package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.data.Parent;
import com.usatiuk.dhfs.objects.transaction.LockingStrategy;
import com.usatiuk.dhfs.objects.transaction.Transaction;
import io.quarkus.logging.Log;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

@QuarkusTest
public class ObjectsTest {
    @Inject
    TransactionManager txm;

    @Inject
    Transaction curTx;

    @Test
    void createObject() {
        {
            txm.begin();
            var newParent = new Parent(JObjectKey.of("Parent"), "John");
            curTx.put(newParent);
            txm.commit();
        }

        {
            txm.begin();
            var parent = curTx.get(Parent.class, JObjectKey.of("Parent")).orElse(null);
            Assertions.assertEquals("John", parent.name());
            txm.commit();
        }
    }

    @Test
    void createGetObject() {
        {
            txm.begin();
            var newParent = new Parent(JObjectKey.of("ParentCreateGet"), "John");
            curTx.put(newParent);
            var parent = curTx.get(Parent.class, JObjectKey.of("ParentCreateGet")).orElse(null);
            Assertions.assertEquals("John", parent.name());
            txm.commit();
        }

        {
            txm.begin();
            var parent = curTx.get(Parent.class, JObjectKey.of("ParentCreateGet")).orElse(null);
            Assertions.assertEquals("John", parent.name());
            txm.commit();
        }
    }

    @Test
    void createDeleteObject() {
        {
            txm.begin();
            var newParent = new Parent(JObjectKey.of("Parent2"), "John");
            curTx.put(newParent);
            txm.commit();
        }

        {
            txm.begin();
            var parent = curTx.get(Parent.class, JObjectKey.of("Parent2")).orElse(null);
            Assertions.assertEquals("John", parent.name());
            txm.commit();
        }

        {
            txm.begin();
            curTx.delete(new JObjectKey("Parent2"));
            txm.commit();
        }

        {
            txm.begin();
            var parent = curTx.get(Parent.class, new JObjectKey("Parent2")).orElse(null);
            Assertions.assertNull(parent);
            txm.commit();
        }
    }

    @Test
    void createCreateObject() {
        {
            txm.begin();
            var newParent = new Parent(JObjectKey.of("Parent7"), "John");
            curTx.put(newParent);
            txm.commit();
        }
        {
            txm.begin();
            var newParent = new Parent(JObjectKey.of("Parent7"), "John2");
            curTx.put(newParent);
            txm.commit();
        }
        {
            txm.begin();
            var parent = curTx.get(Parent.class, new JObjectKey("Parent7")).orElse(null);
            Assertions.assertEquals("John2", parent.name());
            txm.commit();
        }
    }

    @Test
    void editObject() {
        {
            txm.begin();
            var newParent = new Parent(JObjectKey.of("Parent3"), "John");
            curTx.put(newParent);
            txm.commit();
        }

        {
            txm.begin();
            var parent = curTx.get(Parent.class, new JObjectKey("Parent3"), LockingStrategy.OPTIMISTIC).orElse(null);
            Assertions.assertEquals("John", parent.name());
            curTx.put(parent.toBuilder().name("John2").build());
            txm.commit();
        }

        {
            txm.begin();
            var parent = curTx.get(Parent.class, new JObjectKey("Parent3"), LockingStrategy.WRITE).orElse(null);
            Assertions.assertEquals("John2", parent.name());
            curTx.put(parent.toBuilder().name("John3").build());
            txm.commit();
        }

        {
            txm.begin();
            var parent = curTx.get(Parent.class, new JObjectKey("Parent3")).orElse(null);
            Assertions.assertEquals("John3", parent.name());
            txm.commit();
        }
    }

    @Test
    void createObjectConflict() throws InterruptedException {
        AtomicBoolean thread1Failed = new AtomicBoolean(true);
        AtomicBoolean thread2Failed = new AtomicBoolean(true);

        var barrier = new CyclicBarrier(2);
        var latch = new CountDownLatch(2);

        Just.run(() -> {
            try {
                Log.warn("Thread 1");
                txm.begin();
                barrier.await();
                var got = curTx.get(Parent.class, new JObjectKey("Parent2")).orElse(null);
                var newParent = new Parent(JObjectKey.of("Parent2"), "John");
                curTx.put(newParent);
                Log.warn("Thread 1 commit");
                txm.commit();
                thread1Failed.set(false);
                return null;
            } finally {
                latch.countDown();
            }
        });
        Just.run(() -> {
            try {
                Log.warn("Thread 2");
                txm.begin();
                barrier.await();
                var got = curTx.get(Parent.class, new JObjectKey("Parent2")).orElse(null);
                var newParent = new Parent(JObjectKey.of("Parent2"), "John2");
                curTx.put(newParent);
                Log.warn("Thread 2 commit");
                txm.commit();
                thread2Failed.set(false);
                return null;
            } finally {
                latch.countDown();
            }
        });

        latch.await();

        txm.begin();
        var got = curTx.get(Parent.class, new JObjectKey("Parent2")).orElse(null);
        txm.commit();

        if (!thread1Failed.get()) {
            Assertions.assertTrue(thread2Failed.get());
            Assertions.assertEquals("John", got.name());
        } else if (!thread2Failed.get()) {
            Assertions.assertEquals("John2", got.name());
        } else {
            Assertions.fail("No thread succeeded");
        }
    }

    @ParameterizedTest
    @EnumSource(LockingStrategy.class)
    void editConflict(LockingStrategy strategy) throws InterruptedException {
        String key = "Parent4" + strategy.name();
        {
            txm.begin();
            var newParent = new Parent(JObjectKey.of(key), "John3");
            curTx.put(newParent);
            txm.commit();
        }

        AtomicBoolean thread1Failed = new AtomicBoolean(true);
        AtomicBoolean thread2Failed = new AtomicBoolean(true);

        var barrier = new CyclicBarrier(2);
        var latchEnd = new CountDownLatch(2);

        Just.run(() -> {
            try {
                Log.warn("Thread 1");
                txm.begin();
                barrier.await();
                var parent = curTx.get(Parent.class, new JObjectKey(key), strategy).orElse(null);
                curTx.put(parent.toBuilder().name("John").build());
                Log.warn("Thread 1 commit");
                txm.commit();
                Log.warn("Thread 1 commit done");
                thread1Failed.set(false);
                return null;
            } finally {
                latchEnd.countDown();
            }
        });
        Just.run(() -> {
            try {
                Log.warn("Thread 2");
                txm.begin();
                barrier.await();
                var parent = curTx.get(Parent.class, new JObjectKey(key), strategy).orElse(null);
                curTx.put(parent.toBuilder().name("John2").build());
                Log.warn("Thread 2 commit");
                txm.commit();
                Log.warn("Thread 2 commit done");
                thread2Failed.set(false);
                return null;
            } finally {
                latchEnd.countDown();
            }
        });

        latchEnd.await();

        txm.begin();
        var got = curTx.get(Parent.class, new JObjectKey(key)).orElse(null);
        txm.commit();

        if (!thread1Failed.get()) {
            Assertions.assertTrue(thread2Failed.get());
            Assertions.assertEquals("John", got.name());
        } else if (!thread2Failed.get()) {
            Assertions.assertEquals("John2", got.name());
        } else {
            Assertions.fail("No thread succeeded");
        }

        Assertions.assertTrue(thread1Failed.get() || thread2Failed.get());
    }

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
