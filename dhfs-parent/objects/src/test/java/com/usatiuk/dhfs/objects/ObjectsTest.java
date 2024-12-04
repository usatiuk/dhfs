package com.usatiuk.dhfs.objects;

import com.usatiuk.dhfs.objects.data.Parent;
import com.usatiuk.dhfs.objects.transaction.LockingStrategy;
import com.usatiuk.dhfs.objects.transaction.Transaction;
import com.usatiuk.objects.alloc.runtime.ObjectAllocator;
import com.usatiuk.objects.common.runtime.JObjectKey;
import io.quarkus.logging.Log;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

@QuarkusTest
public class ObjectsTest {
    @Inject
    TransactionManager txm;

    @Inject
    Transaction curTx;

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
            var parent = curTx.getObject(Parent.class, new JObjectKey("Parent"), LockingStrategy.READ).orElse(null);
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
            var parent = curTx.getObject(Parent.class, new JObjectKey("Parent3"), LockingStrategy.READ).orElse(null);
            Assertions.assertEquals("John3", parent.getLastName());
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
                var newParent = alloc.create(Parent.class, new JObjectKey("Parent2"));
                newParent.setLastName("John");
                curTx.putObject(newParent);
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
                var newParent = alloc.create(Parent.class, new JObjectKey("Parent2"));
                newParent.setLastName("John2");
                curTx.putObject(newParent);
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
        var got = curTx.getObject(Parent.class, new JObjectKey("Parent2"), LockingStrategy.READ).orElse(null);
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

    @Test
    void editConflict() throws InterruptedException {
        {
            txm.begin();
            var newParent = alloc.create(Parent.class, new JObjectKey("Parent4"));
            newParent.setLastName("John3");
            curTx.putObject(newParent);
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
                var parent = curTx.getObject(Parent.class, new JObjectKey("Parent4"), LockingStrategy.OPTIMISTIC).orElse(null);
                parent.setLastName("John");
                Log.warn("Thread 1 commit");
                txm.commit();
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
                var parent = curTx.getObject(Parent.class, new JObjectKey("Parent4"), LockingStrategy.OPTIMISTIC).orElse(null);
                parent.setLastName("John2");
                Log.warn("Thread 2 commit");
                txm.commit();
                thread2Failed.set(false);
                return null;
            } finally {
                latchEnd.countDown();
            }
        });

        latchEnd.await();

        txm.begin();
        var got = curTx.getObject(Parent.class, new JObjectKey("Parent4"), LockingStrategy.READ).orElse(null);
        txm.commit();

        if (!thread1Failed.get()) {
            Assertions.assertTrue(thread2Failed.get());
            Assertions.assertEquals("John", got.getLastName());
        } else if (!thread2Failed.get()) {
            Assertions.assertEquals("John2", got.getLastName());
        } else {
            Assertions.fail("No thread succeeded");
        }

        Assertions.assertTrue(thread1Failed.get() || thread2Failed.get());
    }

    @Test
    void editLock() throws InterruptedException {
        {
            txm.begin();
            var newParent = alloc.create(Parent.class, new JObjectKey("Parent5"));
            newParent.setLastName("John3");
            curTx.putObject(newParent);
            txm.commit();
        }

        AtomicBoolean thread1Failed = new AtomicBoolean(true);
        AtomicBoolean thread2Failed = new AtomicBoolean(true);

        var barrier = new CyclicBarrier(2);
        var latch = new CountDownLatch(2);

        Just.run(() -> {
            try {
                Log.warn("Thread 1");
                txm.begin();
                barrier.await();
                var parent = curTx.getObject(Parent.class, new JObjectKey("Parent5"), LockingStrategy.WRITE).orElse(null);
                parent.setLastName("John");
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
                var parent = curTx.getObject(Parent.class, new JObjectKey("Parent5"), LockingStrategy.WRITE).orElse(null);
                parent.setLastName("John2");
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
        var got = curTx.getObject(Parent.class, new JObjectKey("Parent5"), LockingStrategy.READ).orElse(null);
        txm.commit();

        Assertions.assertTrue(!thread1Failed.get() && !thread2Failed.get());
        Assertions.assertTrue(got.getLastName().equals("John") || got.getLastName().equals("John2"));
    }

    @Test
    @Disabled // Doesn't work as "lastWrittenTx" is not persistent
    void editLockSerializable() throws InterruptedException {
        {
            txm.begin();
            var newParent = alloc.create(Parent.class, new JObjectKey("Parent6"));
            newParent.setLastName("John3");
            curTx.putObject(newParent);
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
                var parent = curTx.getObject(Parent.class, new JObjectKey("Parent6"), LockingStrategy.WRITE_SERIALIZABLE).orElse(null);
                parent.setLastName("John");
                Log.warn("Thread 1 commit");
                txm.commit();
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
                var parent = curTx.getObject(Parent.class, new JObjectKey("Parent6"), LockingStrategy.WRITE_SERIALIZABLE).orElse(null);
                parent.setLastName("John2");
                Log.warn("Thread 2 commit");
                txm.commit();
                thread2Failed.set(false);
                return null;
            } finally {
                latchEnd.countDown();
            }
        });

        latchEnd.await();

        txm.begin();
        var got = curTx.getObject(Parent.class, new JObjectKey("Parent6"), LockingStrategy.READ).orElse(null);
        txm.commit();

        if (!thread1Failed.get()) {
            Assertions.assertTrue(thread2Failed.get());
            Assertions.assertEquals("John", got.getLastName());
        } else if (!thread2Failed.get()) {
            Assertions.assertEquals("John2", got.getLastName());
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
