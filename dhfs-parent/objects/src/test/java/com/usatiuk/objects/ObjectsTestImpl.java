package com.usatiuk.objects;

import com.usatiuk.objects.data.Parent;
import com.usatiuk.objects.iterators.IteratorStart;
import com.usatiuk.objects.transaction.LockingStrategy;
import com.usatiuk.objects.transaction.Transaction;
import com.usatiuk.objects.transaction.TransactionManager;
import io.quarkus.logging.Log;
import jakarta.inject.Inject;
import org.junit.jupiter.api.*;
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
    void createObject(TestInfo testInfo) {
        txm.run(() -> {
            var newParent = new Parent(JObjectKey.of(testInfo.getDisplayName() + "ParentCreate"), "John");
            curTx.put(newParent);
        });

        txm.run(() -> {
            var parent = curTx.get(Parent.class, JObjectKey.of(testInfo.getDisplayName() + "ParentCreate")).orElse(null);
            Assertions.assertEquals("John", parent.name());
        });
    }

    @Test
    void onCommitHookTest(TestInfo testInfo) {
        txm.run(() -> {
            var newParent = new Parent(JObjectKey.of(testInfo.getDisplayName() + "ParentOnCommitHook"), "John");
            curTx.put(newParent);
            curTx.onCommit(() -> txm.run(() -> {
                curTx.put(new Parent(JObjectKey.of(testInfo.getDisplayName() + "ParentOnCommitHook2"), "John2"));
            }));
        });
        txm.run(() -> {
            curTx.onCommit(() -> txm.run(() -> {
                curTx.put(new Parent(JObjectKey.of(testInfo.getDisplayName() + "ParentOnCommitHook3"), "John3"));
            }));
        });
        txm.run(() -> {
            var parent = curTx.get(Parent.class, JObjectKey.of(testInfo.getDisplayName() + "ParentOnCommitHook")).orElse(null);
            Assertions.assertEquals("John", parent.name());
            var parent2 = curTx.get(Parent.class, JObjectKey.of(testInfo.getDisplayName() + "ParentOnCommitHook2")).orElse(null);
            Assertions.assertEquals("John2", parent2.name());
            var parent3 = curTx.get(Parent.class, JObjectKey.of(testInfo.getDisplayName() + "ParentOnCommitHook3")).orElse(null);
            Assertions.assertEquals("John3", parent3.name());
        });
    }

    @Test
    void createGetObject(TestInfo testInfo) {
        txm.run(() -> {
            var newParent = new Parent(JObjectKey.of(testInfo.getDisplayName() + "ParentCreateGet"), "John");
            curTx.put(newParent);
            var parent = curTx.get(Parent.class, JObjectKey.of(testInfo.getDisplayName() + "ParentCreateGet")).orElse(null);
            Assertions.assertEquals("John", parent.name());
        });

        txm.run(() -> {
            var parent = curTx.get(Parent.class, JObjectKey.of(testInfo.getDisplayName() + "ParentCreateGet")).orElse(null);
            Assertions.assertEquals("John", parent.name());
        });
    }

    @RepeatedTest(100)
    void createDeleteObject(TestInfo testInfo) {
        txm.run(() -> {
            var newParent = new Parent(JObjectKey.of(testInfo.getDisplayName() + "ParentCreateDeleteObject"), "John");
            curTx.put(newParent);
        });

        txm.run(() -> {
            var parent = curTx.get(Parent.class, JObjectKey.of(testInfo.getDisplayName() + "ParentCreateDeleteObject")).orElse(null);
            Assertions.assertEquals("John", parent.name());
        });

        txm.run(() -> {
            curTx.delete(JObjectKey.of(testInfo.getDisplayName() + "ParentCreateDeleteObject"));
        });

        txm.run(() -> {
            var parent = curTx.get(Parent.class, JObjectKey.of(testInfo.getDisplayName() + "ParentCreateDeleteObject")).orElse(null);
            Assertions.assertNull(parent);
        });
    }

    @Test
    void createCreateObject(TestInfo testInfo) {
        txm.run(() -> {
            var newParent = new Parent(JObjectKey.of(testInfo.getDisplayName() + "Parent7"), "John");
            curTx.put(newParent);
        });
        txm.run(() -> {
            var newParent = new Parent(JObjectKey.of(testInfo.getDisplayName() + "Parent7"), "John2");
            curTx.put(newParent);
        });
        txm.run(() -> {
            var parent = curTx.get(Parent.class, JObjectKey.of(testInfo.getDisplayName() + "Parent7")).orElse(null);
            Assertions.assertEquals("John2", parent.name());
        });
    }

    @Test
    void editObject(TestInfo testInfo) {
        txm.run(() -> {
            var newParent = new Parent(JObjectKey.of(testInfo.getDisplayName() + "Parent3"), "John");
            curTx.put(newParent);
        });

        txm.run(() -> {
            var parent = curTx.get(Parent.class, JObjectKey.of(testInfo.getDisplayName() + "Parent3"), LockingStrategy.OPTIMISTIC).orElse(null);
            Assertions.assertEquals("John", parent.name());
            curTx.put(parent.withName("John2"));
        });
        txm.run(() -> {
            var parent = curTx.get(Parent.class, JObjectKey.of(testInfo.getDisplayName() + "Parent3"), LockingStrategy.WRITE).orElse(null);
            Assertions.assertEquals("John2", parent.name());
            curTx.put(parent.withName("John3"));
        });
        txm.run(() -> {
            var parent = curTx.get(Parent.class, JObjectKey.of(testInfo.getDisplayName() + "Parent3")).orElse(null);
            Assertions.assertEquals("John3", parent.name());
        });
    }

    @Test
    @Disabled
    void createObjectConflict(TestInfo testInfo) {
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
                    var got = curTx.get(Parent.class, JObjectKey.of(testInfo.getDisplayName() + "Parent2")).orElse(null);
                    var newParent = new Parent(JObjectKey.of(testInfo.getDisplayName() + "Parent2"), "John");
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
                    var got = curTx.get(Parent.class, JObjectKey.of(testInfo.getDisplayName() + "Parent2")).orElse(null);
                    var newParent = new Parent(JObjectKey.of(testInfo.getDisplayName() + "Parent2"), "John2");
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
            return curTx.get(Parent.class, JObjectKey.of(testInfo.getDisplayName() + "Parent2")).orElse(null);
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
    void editConflict(LockingStrategy strategy, TestInfo testInfo) {
        String key = testInfo.getDisplayName() + "Parent4" + strategy.name();
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
                    var parent = curTx.get(Parent.class, JObjectKey.of(key), strategy).orElse(null);
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
                    var parent = curTx.get(Parent.class, JObjectKey.of(key), strategy).orElse(null);
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
            return curTx.get(Parent.class, JObjectKey.of(key)).orElse(null);
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
    void editConflict2(LockingStrategy strategy, TestInfo testInfo) {
        String key = testInfo.getDisplayName() + "EditConflict2" + strategy.name();
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
                    var parent = curTx.get(Parent.class, JObjectKey.of(key), strategy).orElse(null);
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
                    var parent = curTx.get(Parent.class, JObjectKey.of(key), strategy).orElse(null);
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
            return curTx.get(Parent.class, JObjectKey.of(key)).orElse(null);
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
    void snapshotTest1(TestInfo testInfo) {
        var key = testInfo.getDisplayName() + "SnapshotTest1";
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
                            Assertions.assertTrue(curTx.get(Parent.class, JObjectKey.of(key)).isEmpty());
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
            Assertions.assertEquals("John", curTx.get(Parent.class, JObjectKey.of(key)).orElseThrow().name());
        });
        deleteAndCheck(JObjectKey.of(key));
    }

    @RepeatedTest(100)
    void snapshotTest2(TestInfo testInfo) {
        var key = testInfo.getDisplayName() + "SnapshotTest2";
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
                            Assertions.assertEquals("John", curTx.get(Parent.class, JObjectKey.of(key)).orElseThrow().name());
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
            Assertions.assertEquals("John2", curTx.get(Parent.class, JObjectKey.of(key)).orElseThrow().name());
        });
        deleteAndCheck(JObjectKey.of(key));
    }

    @RepeatedTest(100)
    void snapshotTest3(TestInfo testInfo) {
        var key = testInfo.getDisplayName() + "SnapshotTest3";
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
                            Assertions.assertEquals("John", curTx.get(Parent.class, JObjectKey.of(key)).orElseThrow().name());
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
            Assertions.assertEquals("John2", curTx.get(Parent.class, JObjectKey.of(key)).orElseThrow().name());
        });
        deleteAndCheck(JObjectKey.of(key));
    }

    @RepeatedTest(100)
    void simpleIterator1(TestInfo testInfo) {
        var key = testInfo.getDisplayName() + "SimpleIterator1";
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
            var iter = curTx.getIterator(IteratorStart.GT, JObjectKey.of(key));
            var got = iter.next();
            Assertions.assertEquals(key1, got.getKey().value());
            got = iter.next();
            Assertions.assertEquals(key2, got.getKey().value());
            got = iter.next();
            Assertions.assertEquals(key3, got.getKey().value());
            got = iter.next();
            Assertions.assertEquals(key4, got.getKey().value());
            iter.close();
        });
    }

    @RepeatedTest(100)
    void simpleIterator2(TestInfo testInfo) {
        var key = testInfo.getDisplayName() + "SimpleIterator2";
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
            try (var iter = curTx.getIterator(IteratorStart.GT, JObjectKey.of(key))) {
                var got = iter.next();
                Assertions.assertEquals(key1, got.getKey().value());
                got = iter.next();
                Assertions.assertEquals(key2, got.getKey().value());
                got = iter.next();
                Assertions.assertEquals(key3, got.getKey().value());
                got = iter.next();
                Assertions.assertEquals(key4, got.getKey().value());
            }
        });
        txm.run(() -> {
            try (var iter = curTx.getIterator(IteratorStart.LT, JObjectKey.of(key + "_5"))) {
                var got = iter.next();
                Assertions.assertEquals(key4, got.getKey().value());
                Assertions.assertTrue(iter.hasPrev());
                got = iter.prev();
                Assertions.assertEquals(key4, got.getKey().value());
                Assertions.assertTrue(iter.hasNext());
                got = iter.next();
                Assertions.assertEquals(key4, got.getKey().value());
            }
        });
        txm.run(() -> {
            curTx.delete(JObjectKey.of(key));
            curTx.delete(JObjectKey.of(key1));
            curTx.delete(JObjectKey.of(key2));
            curTx.delete(JObjectKey.of(key3));
            curTx.delete(JObjectKey.of(key4));
        });
        txm.run(() -> {
            try (var iter = curTx.getIterator(IteratorStart.GT, JObjectKey.of(key))) {
                Assertions.assertTrue(!iter.hasNext() || !iter.next().getKey().value().startsWith(key));
            }
        });
    }

    @RepeatedTest(100)
    void concurrentIterator1(TestInfo testInfo) {
        var key = testInfo.getDisplayName() + "ConcurrentIterator1";
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
                    try (var iter = curTx.getIterator(IteratorStart.GT, JObjectKey.of(key))) {
                        var got = iter.next();
                        Assertions.assertEquals(key1, got.getKey().value());
                        got = iter.next();
                        Assertions.assertEquals(key4, got.getKey().value());
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
            try (var iter = curTx.getIterator(IteratorStart.GT, JObjectKey.of(key))) {
                var got = iter.next();
                Assertions.assertEquals(key1, got.getKey().value());
                got = iter.next();
                Assertions.assertEquals(key2, got.getKey().value());
                got = iter.next();
                Assertions.assertEquals(key3, got.getKey().value());
                got = iter.next();
                Assertions.assertEquals(key4, got.getKey().value());
            }
        });
        txm.run(() -> {
            curTx.delete(JObjectKey.of(key));
            curTx.delete(JObjectKey.of(key1));
            curTx.delete(JObjectKey.of(key2));
            curTx.delete(JObjectKey.of(key3));
            curTx.delete(JObjectKey.of(key4));
        });
        txm.run(() -> {
            try (var iter = curTx.getIterator(IteratorStart.GT, JObjectKey.of(key))) {
                Assertions.assertTrue(!iter.hasNext() || !iter.next().getKey().value().startsWith(key));
            }
        });
    }

    @RepeatedTest(100)
    void concurrentIterator2(TestInfo testInfo) {
        var key = testInfo.getDisplayName() + "ConcurrentIterator2";
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
                    try (var iter = curTx.getIterator(IteratorStart.GT, JObjectKey.of(key))) {
                        var got = iter.next();
                        Assertions.assertEquals(key1, got.getKey().value());
                        got = iter.next();
                        Assertions.assertEquals(key2, got.getKey().value());
                        Assertions.assertEquals("John2", ((Parent) got.getValue()).name());
                        got = iter.next();
                        Assertions.assertEquals(key4, got.getKey().value());
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
            try (var iter = curTx.getIterator(IteratorStart.GT, JObjectKey.of(key))) {
                var got = iter.next();
                Assertions.assertEquals(key1, got.getKey().value());
                got = iter.next();
                Assertions.assertEquals(key2, got.getKey().value());
                Assertions.assertEquals("John5", ((Parent) got.getValue()).name());
                got = iter.next();
                Assertions.assertEquals(key3, got.getKey().value());
                got = iter.next();
                Assertions.assertEquals(key4, got.getKey().value());
            }
        });
        txm.run(() -> {
            curTx.delete(JObjectKey.of(key));
            curTx.delete(JObjectKey.of(key1));
            curTx.delete(JObjectKey.of(key2));
            curTx.delete(JObjectKey.of(key3));
            curTx.delete(JObjectKey.of(key4));
        });
        txm.run(() -> {
            try (var iter = curTx.getIterator(IteratorStart.GT, JObjectKey.of(key))) {
                Assertions.assertTrue(!iter.hasNext() || !iter.next().getKey().value().startsWith(key));
            }
        });
    }

    @RepeatedTest(100)
    void concurrentIterator3(TestInfo testInfo) {
        var key = testInfo.getDisplayName() + "ConcurrentIterator3";
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
                curTx.delete(JObjectKey.of(key2));
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
                    try (var iter = curTx.getIterator(IteratorStart.LE, JObjectKey.of(key3))) {
                        var got = iter.next();
                        Assertions.assertEquals(key2, got.getKey().value());
                        Assertions.assertEquals("John2", ((Parent) got.getValue()).name());
                        Assertions.assertTrue(iter.hasNext());
                        Assertions.assertTrue(iter.hasPrev());
                        got = iter.next();
                        Assertions.assertEquals(key4, got.getKey().value());
                        Assertions.assertTrue(iter.hasPrev());
                        got = iter.prev();
                        Assertions.assertEquals(key4, got.getKey().value());
                        Assertions.assertTrue(iter.hasPrev());
                        got = iter.prev();
                        Assertions.assertEquals("John2", ((Parent) got.getValue()).name());
                        Assertions.assertTrue(iter.hasPrev());
                        got = iter.prev();
                        Assertions.assertEquals(key1, got.getKey().value());
                        Assertions.assertTrue(iter.hasNext());
                        got = iter.next();
                        Assertions.assertEquals(key1, got.getKey().value());
                        got = iter.next();
                        Assertions.assertEquals(key2, got.getKey().value());
                        Assertions.assertEquals("John2", ((Parent) got.getValue()).name());
                        got = iter.next();
                        Assertions.assertEquals(key4, got.getKey().value());
                    }
                    try (var iter = curTx.getIterator(IteratorStart.GT, JObjectKey.of(key))) {
                        var got = iter.next();
                        Assertions.assertEquals(key1, got.getKey().value());
                        got = iter.next();
                        Assertions.assertEquals(key2, got.getKey().value());
                        Assertions.assertEquals("John2", ((Parent) got.getValue()).name());
                        got = iter.next();
                        Assertions.assertEquals(key4, got.getKey().value());
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
            try (var iter = curTx.getIterator(IteratorStart.GT, JObjectKey.of(key))) {
                var got = iter.next();
                Assertions.assertEquals(key1, got.getKey().value());
                got = iter.next();
                Assertions.assertEquals(key3, got.getKey().value());
                got = iter.next();
                Assertions.assertEquals(key4, got.getKey().value());
            }
        });
        txm.run(() -> {
            curTx.delete(JObjectKey.of(key));
            curTx.delete(JObjectKey.of(key1));
            curTx.delete(JObjectKey.of(key3));
            curTx.delete(JObjectKey.of(key4));
        });
        txm.run(() -> {
            try (var iter = curTx.getIterator(IteratorStart.GT, JObjectKey.of(key))) {
                Assertions.assertTrue(!iter.hasNext() || !iter.next().getKey().value().startsWith(key));
            }
        });
    }

    @RepeatedTest(100)
    void allParallel(TestInfo testInfo) {
        Just.runAll(
                () -> createObject(testInfo),
                () -> createGetObject(testInfo),
                () -> createDeleteObject(testInfo),
                () -> createCreateObject(testInfo),
                () -> editConflict(LockingStrategy.WRITE, testInfo),
                () -> editConflict(LockingStrategy.OPTIMISTIC, testInfo),
                () -> editConflict2(LockingStrategy.WRITE, testInfo),
                () -> editConflict2(LockingStrategy.OPTIMISTIC, testInfo),
                () -> snapshotTest1(testInfo),
                () -> snapshotTest2(testInfo),
                () -> snapshotTest3(testInfo),
                () -> simpleIterator1(testInfo),
                () -> simpleIterator2(testInfo),
                () -> concurrentIterator1(testInfo),
                () -> concurrentIterator2(testInfo),
                () -> concurrentIterator3(testInfo)
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
