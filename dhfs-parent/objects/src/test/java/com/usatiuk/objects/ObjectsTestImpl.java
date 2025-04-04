package com.usatiuk.objects;

import com.usatiuk.objects.data.Parent;
import com.usatiuk.objects.iterators.IteratorStart;
import com.usatiuk.objects.transaction.LockingStrategy;
import com.usatiuk.objects.transaction.Transaction;
import com.usatiuk.objects.transaction.TransactionManager;
import io.quarkus.logging.Log;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

class Profiles {
    public static class ObjectsTestProfileExtraChecks extends TempDataProfile {
        @Override
        protected void getConfigOverrides(Map<String, String> toPut) {
            toPut.put("dhfs.objects.persistence.snapshot-extra-checks", "true");
        }
    }

    public static class ObjectsTestProfileNoExtraChecks extends TempDataProfile {
        @Override
        protected void getConfigOverrides(Map<String, String> toPut) {
            toPut.put("dhfs.objects.persistence.snapshot-extra-checks", "false");
        }
    }
}

public abstract class ObjectsTestImpl {
    @Inject
    TransactionManager txm;

    @Inject
    Transaction curTx;

    private void deleteAndCheck(JObjectKey key) {
        txm.run(() -> {
            curTx.delete(key);
        });

        txm.run(() -> {
            var parent = curTx.get(JData.class, key).orElse(null);
            Assertions.assertNull(parent);
        });
    }

    @Test
    void createObject() {
        txm.run(() -> {
            var newParent = new Parent(JObjectKey.of("ParentCreate"), "John");
            curTx.put(newParent);
        });

        txm.run(() -> {
            var parent = curTx.get(Parent.class, new JObjectKeyImpl("ParentCreate")).orElse(null);
            Assertions.assertEquals("John", parent.name());
        });
    }

    @Test
    void onCommitHookTest() {
        txm.run(() -> {
            var newParent = new Parent(JObjectKey.of("ParentOnCommitHook"), "John");
            curTx.put(newParent);
            curTx.onCommit(() -> txm.run(() -> {
                curTx.put(new Parent(JObjectKey.of("ParentOnCommitHook2"), "John2"));
            }));
        });
        txm.run(() -> {
            curTx.onCommit(() -> txm.run(() -> {
                curTx.put(new Parent(JObjectKey.of("ParentOnCommitHook3"), "John3"));
            }));
        });
        txm.run(() -> {
            var parent = curTx.get(Parent.class, new JObjectKeyImpl("ParentOnCommitHook")).orElse(null);
            Assertions.assertEquals("John", parent.name());
            var parent2 = curTx.get(Parent.class, new JObjectKeyImpl("ParentOnCommitHook2")).orElse(null);
            Assertions.assertEquals("John2", parent2.name());
            var parent3 = curTx.get(Parent.class, new JObjectKeyImpl("ParentOnCommitHook3")).orElse(null);
            Assertions.assertEquals("John3", parent3.name());
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
            var parent = curTx.get(Parent.class, new JObjectKeyImpl("ParentCreateGet")).orElse(null);
            Assertions.assertEquals("John", parent.name());
        });
    }

    @RepeatedTest(100)
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
            curTx.delete(new JObjectKeyImpl("ParentCreateDeleteObject"));
        });

        txm.run(() -> {
            var parent = curTx.get(Parent.class, new JObjectKeyImpl("ParentCreateDeleteObject")).orElse(null);
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
            var parent = curTx.get(Parent.class, new JObjectKeyImpl("Parent7")).orElse(null);
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
            var parent = curTx.get(Parent.class, new JObjectKeyImpl("Parent3"), LockingStrategy.OPTIMISTIC).orElse(null);
            Assertions.assertEquals("John", parent.name());
            curTx.put(parent.withName("John2"));
        });
        txm.run(() -> {
            var parent = curTx.get(Parent.class, new JObjectKeyImpl("Parent3"), LockingStrategy.WRITE).orElse(null);
            Assertions.assertEquals("John2", parent.name());
            curTx.put(parent.withName("John3"));
        });
        txm.run(() -> {
            var parent = curTx.get(Parent.class, new JObjectKeyImpl("Parent3")).orElse(null);
            Assertions.assertEquals("John3", parent.name());
        });
    }

    @Test
    @Disabled
    void createObjectConflict() {
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
                    var got = curTx.get(Parent.class, new JObjectKeyImpl("Parent2")).orElse(null);
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
                    var got = curTx.get(Parent.class, new JObjectKeyImpl("Parent2")).orElse(null);
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

        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        var got = txm.run(() -> {
            return curTx.get(Parent.class, new JObjectKeyImpl("Parent2")).orElse(null);
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
    void editConflict(LockingStrategy strategy) {
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
                    var parent = curTx.get(Parent.class, new JObjectKeyImpl(key), strategy).orElse(null);
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
                    var parent = curTx.get(Parent.class, new JObjectKeyImpl(key), strategy).orElse(null);
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

        try {
            latchEnd.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        var got = txm.run(() -> {
            return curTx.get(Parent.class, new JObjectKeyImpl(key)).orElse(null);
        });

        if (!thread1Failed.get() && !thread2Failed.get()) {
            Assertions.assertTrue(got.name().equals("John") || got.name().equals("John2"));
            return;
        }

        Assertions.assertFalse(!thread1Failed.get() && !thread2Failed.get());

        if (!thread1Failed.get()) {
            if (!thread2Failed.get()) {
                Assertions.assertEquals("John2", got.name());
            } else {
                Assertions.assertEquals("John", got.name());
            }
        } else {
            Assertions.assertFalse(thread2Failed.get());
            Assertions.assertEquals("John2", got.name());
        }
    }

    @ParameterizedTest
    @EnumSource(LockingStrategy.class)
    void editConflict2(LockingStrategy strategy) {
        String key = "EditConflict2" + strategy.name();
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
                    var parent = curTx.get(Parent.class, new JObjectKeyImpl(key), strategy).orElse(null);
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
                txm.runTries(() -> {
                    // Ensure they will conflict
                    try {
                        barrier.await();
                    } catch (Throwable e) {
                        throw new RuntimeException(e);
                    }
                    var parent = curTx.get(Parent.class, new JObjectKeyImpl(key), strategy).orElse(null);
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

        try {
            latchEnd.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        var got = txm.run(() -> {
            return curTx.get(Parent.class, new JObjectKeyImpl(key)).orElse(null);
        });

        Assertions.assertFalse(!thread1Failed.get() && !thread2Failed.get());

        if (!thread1Failed.get()) {
            if (!thread2Failed.get()) {
                Assertions.assertEquals("John2", got.name());
            } else {
                Assertions.assertEquals("John", got.name());
            }
        } else {
            Assertions.assertFalse(thread2Failed.get());
            Assertions.assertEquals("John2", got.name());
        }
    }

    @RepeatedTest(100)
    void snapshotTest1() {
        var key = "SnapshotTest1";
        var barrier1 = new CyclicBarrier(2);
        var barrier2 = new CyclicBarrier(2);
        try (ExecutorService ex = Executors.newFixedThreadPool(3)) {
            ex.invokeAll(List.of(
                    () -> {
                        barrier1.await();
                        Log.info("Thread 2 starting tx");
                        txm.run(() -> {
                            Log.info("Thread 2 started tx");
                            curTx.put(new Parent(JObjectKey.of(key), "John"));
                            Log.info("Thread 2 committing");
                        });
                        Log.info("Thread 2 commited");
                        try {
                            barrier2.await();
                        } catch (Throwable e) {
                            throw new RuntimeException(e);
                        }
                        return null;
                    },
                    () -> {
                        Log.info("Thread 1 starting tx");
                        txm.run(() -> {
                            try {
                                Log.info("Thread 1 started tx");
                                barrier1.await();
                                barrier2.await();
                            } catch (Throwable e) {
                                throw new RuntimeException(e);
                            }
                            Log.info("Thread 1 reading");
                            Assertions.assertTrue(curTx.get(Parent.class, new JObjectKeyImpl(key)).isEmpty());
                            Log.info("Thread 1 done reading");
                        });
                        Log.info("Thread 1 finished");
                        return null;
                    }
            )).forEach(f -> {
                try {
                    f.get();
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        txm.run(() -> {
            Assertions.assertEquals("John", curTx.get(Parent.class, new JObjectKeyImpl(key)).orElseThrow().name());
        });
        deleteAndCheck(new JObjectKeyImpl(key));
    }

    @RepeatedTest(100)
    void snapshotTest2() {
        var key = "SnapshotTest2";
        var barrier1 = new CyclicBarrier(2);
        var barrier2 = new CyclicBarrier(2);
        txm.run(() -> {
            curTx.put(new Parent(JObjectKey.of(key), "John"));
        });
        try (ExecutorService ex = Executors.newFixedThreadPool(3)) {
            ex.invokeAll(List.of(
                    () -> {
                        barrier1.await();
                        Log.info("Thread 2 starting tx");
                        txm.run(() -> {
                            Log.info("Thread 2 started tx");
                            curTx.put(new Parent(JObjectKey.of(key), "John2"));
                            Log.info("Thread 2 committing");
                        });
                        Log.info("Thread 2 commited");
                        try {
                            barrier2.await();
                        } catch (Throwable e) {
                            throw new RuntimeException(e);
                        }
                        return null;
                    },
                    () -> {
                        Log.info("Thread 1 starting tx");
                        txm.run(() -> {
                            try {
                                Log.info("Thread 1 started tx");
                                barrier1.await();
                                barrier2.await();
                            } catch (Throwable e) {
                                throw new RuntimeException(e);
                            }
                            Log.info("Thread 1 reading");
                            Assertions.assertEquals("John", curTx.get(Parent.class, new JObjectKeyImpl(key)).orElseThrow().name());
                            Log.info("Thread 1 done reading");
                        });
                        Log.info("Thread 1 finished");
                        return null;
                    }
            )).forEach(f -> {
                try {
                    f.get();
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        txm.run(() -> {
            Assertions.assertEquals("John2", curTx.get(Parent.class, new JObjectKeyImpl(key)).orElseThrow().name());
        });
        deleteAndCheck(new JObjectKeyImpl(key));
    }

    @RepeatedTest(100)
    void snapshotTest3() {
        var key = "SnapshotTest3";
        var barrier0 = new CountDownLatch(1);
        var barrier1 = new CyclicBarrier(2);
        var barrier2 = new CyclicBarrier(2);
        txm.run(() -> {
            curTx.put(new Parent(JObjectKey.of(key), "John"));
        }).onFlush(barrier0::countDown);
        try {
            barrier0.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        try (ExecutorService ex = Executors.newFixedThreadPool(3)) {
            ex.invokeAll(List.of(
                    () -> {
                        barrier1.await();
                        Log.info("Thread 2 starting tx");
                        txm.run(() -> {
                            Log.info("Thread 2 started tx");
                            curTx.put(new Parent(JObjectKey.of(key), "John2"));
                            Log.info("Thread 2 committing");
                        });
                        Log.info("Thread 2 commited");
                        try {
                            barrier2.await();
                        } catch (Throwable e) {
                            throw new RuntimeException(e);
                        }
                        return null;
                    },
                    () -> {
                        Log.info("Thread 1 starting tx");
                        txm.run(() -> {
                            try {
                                Log.info("Thread 1 started tx");
                                barrier1.await();
                                barrier2.await();
                            } catch (Throwable e) {
                                throw new RuntimeException(e);
                            }
                            Log.info("Thread 1 reading");
                            Assertions.assertEquals("John", curTx.get(Parent.class, new JObjectKeyImpl(key)).orElseThrow().name());
                            Log.info("Thread 1 done reading");
                        });
                        Log.info("Thread 1 finished");
                        return null;
                    }
            )).forEach(f -> {
                try {
                    f.get();
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        txm.run(() -> {
            Assertions.assertEquals("John2", curTx.get(Parent.class, new JObjectKeyImpl(key)).orElseThrow().name());
        });
        deleteAndCheck(new JObjectKeyImpl(key));
    }

    @RepeatedTest(100)
    void simpleIterator1() {
        var key = "SimpleIterator1";
        var key1 = key + "_1";
        var key2 = key + "_2";
        var key3 = key + "_3";
        var key4 = key + "_4";
        txm.run(() -> {
            curTx.put(new Parent(JObjectKey.of(key), "John"));
            curTx.put(new Parent(JObjectKey.of(key1), "John1"));
            curTx.put(new Parent(JObjectKey.of(key2), "John2"));
            curTx.put(new Parent(JObjectKey.of(key3), "John3"));
            curTx.put(new Parent(JObjectKey.of(key4), "John4"));
        });
        txm.run(() -> {
            var iter = curTx.getIterator(IteratorStart.GT, new JObjectKeyImpl(key));
            var got = iter.next();
            Assertions.assertEquals(key1, got.getKey().name());
            got = iter.next();
            Assertions.assertEquals(key2, got.getKey().name());
            got = iter.next();
            Assertions.assertEquals(key3, got.getKey().name());
            got = iter.next();
            Assertions.assertEquals(key4, got.getKey().name());
            iter.close();
        });
    }

    @RepeatedTest(100)
    void simpleIterator2() {
        var key = "SimpleIterator2";
        var key1 = key + "_1";
        var key2 = key + "_2";
        var key3 = key + "_3";
        var key4 = key + "_4";
        txm.run(() -> {
            curTx.put(new Parent(JObjectKey.of(key), "John"));
            curTx.put(new Parent(JObjectKey.of(key1), "John1"));
            curTx.put(new Parent(JObjectKey.of(key2), "John2"));
            curTx.put(new Parent(JObjectKey.of(key3), "John3"));
            curTx.put(new Parent(JObjectKey.of(key4), "John4"));
        });
        txm.run(() -> {
            try (var iter = curTx.getIterator(IteratorStart.GT, new JObjectKeyImpl(key))) {
                var got = iter.next();
                Assertions.assertEquals(key1, got.getKey().name());
                got = iter.next();
                Assertions.assertEquals(key2, got.getKey().name());
                got = iter.next();
                Assertions.assertEquals(key3, got.getKey().name());
                got = iter.next();
                Assertions.assertEquals(key4, got.getKey().name());
            }
        });
        txm.run(() -> {
            try (var iter = curTx.getIterator(IteratorStart.LT, new JObjectKeyImpl(key + "_5"))) {
                var got = iter.next();
                Assertions.assertEquals(key4, got.getKey().name());
                Assertions.assertTrue(iter.hasPrev());
                got = iter.prev();
                Assertions.assertEquals(key4, got.getKey().name());
                Assertions.assertTrue(iter.hasNext());
                got = iter.next();
                Assertions.assertEquals(key4, got.getKey().name());
            }
        });
        txm.run(() -> {
            curTx.delete(new JObjectKeyImpl(key));
            curTx.delete(new JObjectKeyImpl(key1));
            curTx.delete(new JObjectKeyImpl(key2));
            curTx.delete(new JObjectKeyImpl(key3));
            curTx.delete(new JObjectKeyImpl(key4));
        });
        txm.run(() -> {
            try (var iter = curTx.getIterator(IteratorStart.GT, new JObjectKeyImpl(key))) {
                Assertions.assertTrue(!iter.hasNext() || !iter.next().getKey().name().startsWith(key));
            }
        });
    }

    @RepeatedTest(100)
    void concurrentIterator1() {
        var key = "ConcurrentIterator1";
        var key1 = key + "_1";
        var key2 = key + "_2";
        var key3 = key + "_3";
        var key4 = key + "_4";
        txm.run(() -> {
            curTx.put(new Parent(JObjectKey.of(key), "John"));
            curTx.put(new Parent(JObjectKey.of(key1), "John1"));
            curTx.put(new Parent(JObjectKey.of(key4), "John4"));
        });
        var barrier = new CyclicBarrier(2);
        var barrier2 = new CyclicBarrier(2);
        Just.runAll(() -> {
            barrier.await();
            txm.run(() -> {
                Log.info("Thread 1 starting tx");
                try {
                    barrier2.await();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                curTx.put(new Parent(JObjectKey.of(key2), "John2"));
                curTx.put(new Parent(JObjectKey.of(key3), "John3"));
                Log.info("Thread 1 committing");
            });
            Log.info("Thread 1 commited");
            return null;
        }, () -> {
            txm.run(() -> {
                Log.info("Thread 2 starting tx");
                try {
                    barrier.await();
                    barrier2.await();
                    try (var iter = curTx.getIterator(IteratorStart.GT, new JObjectKeyImpl(key))) {
                        var got = iter.next();
                        Assertions.assertEquals(key1, got.getKey().name());
                        got = iter.next();
                        Assertions.assertEquals(key4, got.getKey().name());
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            Log.info("Thread 2 finished");
            return null;
        });
        Log.info("All threads finished");
        txm.run(() -> {
            try (var iter = curTx.getIterator(IteratorStart.GT, new JObjectKeyImpl(key))) {
                var got = iter.next();
                Assertions.assertEquals(key1, got.getKey().name());
                got = iter.next();
                Assertions.assertEquals(key2, got.getKey().name());
                got = iter.next();
                Assertions.assertEquals(key3, got.getKey().name());
                got = iter.next();
                Assertions.assertEquals(key4, got.getKey().name());
            }
        });
        txm.run(() -> {
            curTx.delete(new JObjectKeyImpl(key));
            curTx.delete(new JObjectKeyImpl(key1));
            curTx.delete(new JObjectKeyImpl(key2));
            curTx.delete(new JObjectKeyImpl(key3));
            curTx.delete(new JObjectKeyImpl(key4));
        });
        txm.run(() -> {
            try (var iter = curTx.getIterator(IteratorStart.GT, new JObjectKeyImpl(key))) {
                Assertions.assertTrue(!iter.hasNext() || !iter.next().getKey().name().startsWith(key));
            }
        });
    }

    @RepeatedTest(100)
    void concurrentIterator2() {
        var key = "ConcurrentIterator2";
        var key1 = key + "_1";
        var key2 = key + "_2";
        var key3 = key + "_3";
        var key4 = key + "_4";
        txm.run(() -> {
            curTx.put(new Parent(JObjectKey.of(key), "John"));
            curTx.put(new Parent(JObjectKey.of(key1), "John1"));
            curTx.put(new Parent(JObjectKey.of(key2), "John2"));
            curTx.put(new Parent(JObjectKey.of(key4), "John4"));
        });
        var barrier = new CyclicBarrier(2);
        var barrier2 = new CyclicBarrier(2);
        Just.runAll(() -> {
            barrier.await();
            txm.run(() -> {
                Log.info("Thread 1 starting tx");
                try {
                    barrier2.await();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                curTx.put(new Parent(JObjectKey.of(key2), "John5"));
                curTx.put(new Parent(JObjectKey.of(key3), "John3"));
                Log.info("Thread 1 committing");
            });
            Log.info("Thread 1 commited");
            return null;
        }, () -> {
            txm.run(() -> {
                Log.info("Thread 2 starting tx");
                try {
                    barrier.await();
                    barrier2.await();
                    try (var iter = curTx.getIterator(IteratorStart.GT, new JObjectKeyImpl(key))) {
                        var got = iter.next();
                        Assertions.assertEquals(key1, got.getKey().name());
                        got = iter.next();
                        Assertions.assertEquals(key2, got.getKey().name());
                        Assertions.assertEquals("John2", ((Parent) got.getValue()).name());
                        got = iter.next();
                        Assertions.assertEquals(key4, got.getKey().name());
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            Log.info("Thread 2 finished");
            return null;
        });
        Log.info("All threads finished");
        txm.run(() -> {
            try (var iter = curTx.getIterator(IteratorStart.GT, new JObjectKeyImpl(key))) {
                var got = iter.next();
                Assertions.assertEquals(key1, got.getKey().name());
                got = iter.next();
                Assertions.assertEquals(key2, got.getKey().name());
                Assertions.assertEquals("John5", ((Parent) got.getValue()).name());
                got = iter.next();
                Assertions.assertEquals(key3, got.getKey().name());
                got = iter.next();
                Assertions.assertEquals(key4, got.getKey().name());
            }
        });
        txm.run(() -> {
            curTx.delete(new JObjectKeyImpl(key));
            curTx.delete(new JObjectKeyImpl(key1));
            curTx.delete(new JObjectKeyImpl(key2));
            curTx.delete(new JObjectKeyImpl(key3));
            curTx.delete(new JObjectKeyImpl(key4));
        });
        txm.run(() -> {
            try (var iter = curTx.getIterator(IteratorStart.GT, new JObjectKeyImpl(key))) {
                Assertions.assertTrue(!iter.hasNext() || !iter.next().getKey().name().startsWith(key));
            }
        });
    }

    @RepeatedTest(100)
    void concurrentIterator3() {
        var key = "ConcurrentIterator3";
        var key1 = key + "_1";
        var key2 = key + "_2";
        var key3 = key + "_3";
        var key4 = key + "_4";
        txm.run(() -> {
            curTx.put(new Parent(JObjectKey.of(key), "John"));
            curTx.put(new Parent(JObjectKey.of(key1), "John1"));
            curTx.put(new Parent(JObjectKey.of(key2), "John2"));
            curTx.put(new Parent(JObjectKey.of(key4), "John4"));
        });
        var barrier = new CyclicBarrier(2);
        var barrier2 = new CyclicBarrier(2);
        Just.runAll(() -> {
            barrier.await();
            txm.run(() -> {
                Log.info("Thread 1 starting tx");
                try {
                    barrier2.await();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                curTx.put(new Parent(JObjectKey.of(key3), "John3"));
                curTx.delete(new JObjectKeyImpl(key2));
                Log.info("Thread 1 committing");
            });
            Log.info("Thread 1 commited");
            return null;
        }, () -> {
            txm.run(() -> {
                Log.info("Thread 2 starting tx");
                try {
                    barrier.await();
                    barrier2.await();
                    try (var iter = curTx.getIterator(IteratorStart.LE, new JObjectKeyImpl(key3))) {
                        var got = iter.next();
                        Assertions.assertEquals(key2, got.getKey().name());
                        Assertions.assertEquals("John2", ((Parent) got.getValue()).name());
                        Assertions.assertTrue(iter.hasNext());
                        Assertions.assertTrue(iter.hasPrev());
                        got = iter.next();
                        Assertions.assertEquals(key4, got.getKey().name());
                        Assertions.assertTrue(iter.hasPrev());
                        got = iter.prev();
                        Assertions.assertEquals(key4, got.getKey().name());
                        Assertions.assertTrue(iter.hasPrev());
                        got = iter.prev();
                        Assertions.assertEquals("John2", ((Parent) got.getValue()).name());
                        Assertions.assertTrue(iter.hasPrev());
                        got = iter.prev();
                        Assertions.assertEquals(key1, got.getKey().name());
                        Assertions.assertTrue(iter.hasNext());
                        got = iter.next();
                        Assertions.assertEquals(key1, got.getKey().name());
                        got = iter.next();
                        Assertions.assertEquals(key2, got.getKey().name());
                        Assertions.assertEquals("John2", ((Parent) got.getValue()).name());
                        got = iter.next();
                        Assertions.assertEquals(key4, got.getKey().name());
                    }
                    try (var iter = curTx.getIterator(IteratorStart.GT, new JObjectKeyImpl(key))) {
                        var got = iter.next();
                        Assertions.assertEquals(key1, got.getKey().name());
                        got = iter.next();
                        Assertions.assertEquals(key2, got.getKey().name());
                        Assertions.assertEquals("John2", ((Parent) got.getValue()).name());
                        got = iter.next();
                        Assertions.assertEquals(key4, got.getKey().name());
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            Log.info("Thread 2 finished");
            return null;
        });
        Log.info("All threads finished");
        txm.run(() -> {
            try (var iter = curTx.getIterator(IteratorStart.GT, new JObjectKeyImpl(key))) {
                var got = iter.next();
                Assertions.assertEquals(key1, got.getKey().name());
                got = iter.next();
                Assertions.assertEquals(key3, got.getKey().name());
                got = iter.next();
                Assertions.assertEquals(key4, got.getKey().name());
            }
        });
        txm.run(() -> {
            curTx.delete(new JObjectKeyImpl(key));
            curTx.delete(new JObjectKeyImpl(key1));
            curTx.delete(new JObjectKeyImpl(key3));
            curTx.delete(new JObjectKeyImpl(key4));
        });
        txm.run(() -> {
            try (var iter = curTx.getIterator(IteratorStart.GT, new JObjectKeyImpl(key))) {
                Assertions.assertTrue(!iter.hasNext() || !iter.next().getKey().name().startsWith(key));
            }
        });
    }

    @RepeatedTest(100)
    void allParallel() {
        Just.runAll(
                () -> createObject(),
                () -> createGetObject(),
                () -> createDeleteObject(),
                () -> createCreateObject(),
                () -> editConflict(LockingStrategy.WRITE),
                () -> editConflict(LockingStrategy.OPTIMISTIC),
                () -> editConflict2(LockingStrategy.WRITE),
                () -> editConflict2(LockingStrategy.OPTIMISTIC),
                () -> snapshotTest1(),
                () -> snapshotTest2(),
                () -> snapshotTest3(),
                () -> simpleIterator1(),
                () -> simpleIterator2(),
                () -> concurrentIterator1(),
                () -> concurrentIterator2(),
                () -> concurrentIterator3()
        );
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
