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
        txm.run(() -> {
            var newParent = new Parent(JObjectKey.of("ParentCreate"), "John");
            curTx.put(newParent);
        });

        txm.run(() -> {
            var parent = curTx.get(Parent.class, new JObjectKey("ParentCreate")).orElse(null);
            Assertions.assertEquals("John", parent.name());
        });
    }

    @Test
    void createGetObject() {
        txm.run(() -> {
            var newParent = new Parent(JObjectKey.of("ParentCreateGet"), "John");
            curTx.put(newParent);
            var parent = curTx.get(Parent.class, JObjectKey.of("ParentCreateGet")).orElse(null);
            Assertions.assertEquals("John", parent.name());
        });

        txm.run(() -> {
            var parent = curTx.get(Parent.class, new JObjectKey("ParentCreateGet")).orElse(null);
            Assertions.assertEquals("John", parent.name());
        });
    }

    @Test
    void createDeleteObject() {
        txm.run(() -> {
            var newParent = new Parent(JObjectKey.of("ParentCreateDeleteObject"), "John");
            curTx.put(newParent);
        });

        txm.run(() -> {
            var parent = curTx.get(Parent.class, JObjectKey.of("ParentCreateDeleteObject")).orElse(null);
            Assertions.assertEquals("John", parent.name());
        });

        txm.run(() -> {
            curTx.delete(new JObjectKey("ParentCreateDeleteObject"));
        });

        txm.run(() -> {
            var parent = curTx.get(Parent.class, new JObjectKey("ParentCreateDeleteObject")).orElse(null);
            Assertions.assertNull(parent);
        });
    }

    @Test
    void createCreateObject() {
        txm.run(() -> {
            var newParent = new Parent(JObjectKey.of("Parent7"), "John");
            curTx.put(newParent);
        });
        txm.run(() -> {
            var newParent = new Parent(JObjectKey.of("Parent7"), "John2");
            curTx.put(newParent);
        });
        txm.run(() -> {
            var parent = curTx.get(Parent.class, new JObjectKey("Parent7")).orElse(null);
            Assertions.assertEquals("John2", parent.name());
        });
    }

    @Test
    void editObject() {
        txm.run(() -> {
            var newParent = new Parent(JObjectKey.of("Parent3"), "John");
            curTx.put(newParent);
        });

        txm.run(() -> {
            var parent = curTx.get(Parent.class, new JObjectKey("Parent3"), LockingStrategy.OPTIMISTIC).orElse(null);
            Assertions.assertEquals("John", parent.name());
            curTx.put(parent.withName("John2"));
        });
        txm.run(() -> {
            var parent = curTx.get(Parent.class, new JObjectKey("Parent3"), LockingStrategy.WRITE).orElse(null);
            Assertions.assertEquals("John2", parent.name());
            curTx.put(parent.withName("John3"));
        });
        txm.run(() -> {
            var parent = curTx.get(Parent.class, new JObjectKey("Parent3")).orElse(null);
            Assertions.assertEquals("John3", parent.name());
        });
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
                txm.runTries(() -> {
                    try {
                        barrier.await();
                    } catch (Throwable e) {
                        throw new RuntimeException(e);
                    }
                    var got = curTx.get(Parent.class, new JObjectKey("Parent2")).orElse(null);
                    var newParent = new Parent(JObjectKey.of("Parent2"), "John");
                    curTx.put(newParent);
                    Log.warn("Thread 1 commit");
                }, 0);
                thread1Failed.set(false);
                return null;
            } finally {
                latch.countDown();
            }
        });
        Just.run(() -> {
            try {
                Log.warn("Thread 2");
                txm.runTries(() -> {
                    try {
                        barrier.await();
                    } catch (Throwable e) {
                        throw new RuntimeException(e);
                    }
                    var got = curTx.get(Parent.class, new JObjectKey("Parent2")).orElse(null);
                    var newParent = new Parent(JObjectKey.of("Parent2"), "John2");
                    curTx.put(newParent);
                    Log.warn("Thread 2 commit");
                }, 0);
                thread2Failed.set(false);
                return null;
            } finally {
                latch.countDown();
            }
        });

        latch.await();

        var got = txm.run(() -> {
            return curTx.get(Parent.class, new JObjectKey("Parent2")).orElse(null);
        });

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
        txm.run(() -> {
            var newParent = new Parent(JObjectKey.of(key), "John3");
            curTx.put(newParent);
        });

        AtomicBoolean thread1Failed = new AtomicBoolean(true);
        AtomicBoolean thread2Failed = new AtomicBoolean(true);

        var barrier = new CyclicBarrier(2);
        var latchEnd = new CountDownLatch(2);

        Just.run(() -> {
            try {
                Log.warn("Thread 1");
                txm.runTries(() -> {
                    try {
                        barrier.await();
                    } catch (Throwable e) {
                        throw new RuntimeException(e);
                    }
                    var parent = curTx.get(Parent.class, new JObjectKey(key), strategy).orElse(null);
                    curTx.put(parent.withName("John"));
                    Log.warn("Thread 1 commit");
                }, 0);
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
                barrier.await(); // Ensure thread 2 tx id is larger than thread 1
                txm.runTries(() -> {
                    var parent = curTx.get(Parent.class, new JObjectKey(key), strategy).orElse(null);
                    curTx.put(parent.withName("John2"));
                    Log.warn("Thread 2 commit");
                }, 0);
                Log.warn("Thread 2 commit done");
                thread2Failed.set(false);
                return null;
            } finally {
                latchEnd.countDown();
            }
        });

        latchEnd.await();

        var got = txm.run(() -> {
            return curTx.get(Parent.class, new JObjectKey(key)).orElse(null);
        });

        // It is possible that thread 2 did get the object after thread 1 committed it, so there is no conflict
        Assertions.assertTrue(!thread1Failed.get() || !thread2Failed.get());

        if (strategy.equals(LockingStrategy.WRITE)) {
            if (!thread1Failed.get())
                Assertions.assertFalse(thread2Failed.get());
        }

        if (!thread1Failed.get()) {
            if (!thread2Failed.get()) {
                Assertions.assertEquals("John2", got.name());
            } else {
                Assertions.assertEquals("John", got.name());
            }
        } else {
            Assertions.assertTrue(!thread2Failed.get());
            Assertions.assertEquals("John2", got.name());
        }
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
