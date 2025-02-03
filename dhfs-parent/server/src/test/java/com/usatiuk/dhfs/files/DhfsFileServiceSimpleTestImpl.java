package com.usatiuk.dhfs.files;

import com.usatiuk.dhfs.TempDataProfile;
import com.usatiuk.dhfs.files.objects.ChunkData;
import com.usatiuk.dhfs.files.objects.File;
import com.usatiuk.dhfs.files.service.DhfsFileService;
import com.usatiuk.dhfs.objects.RemoteTransaction;
import com.usatiuk.dhfs.objects.TransactionManager;
import com.usatiuk.dhfs.objects.transaction.Transaction;
import com.usatiuk.kleppmanntree.AlreadyExistsException;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

class Profiles {
    public static class DhfsFileServiceSimpleTestProfile extends TempDataProfile {
        @Override
        protected void getConfigOverrides(Map<String, String> ret) {
            ret.put("dhfs.fuse.enabled", "false");
        }
    }

    public static class DhfsFileServiceSimpleTestProfileNoChunking extends TempDataProfile {
        @Override
        protected void getConfigOverrides(Map<String, String> ret) {
            ret.put("dhfs.fuse.enabled", "false");
            ret.put("dhfs.files.target_chunk_size", "-1");
        }
    }

    public static class DhfsFileServiceSimpleTestProfileSmallChunking extends TempDataProfile {
        @Override
        protected void getConfigOverrides(Map<String, String> ret) {
            ret.put("dhfs.fuse.enabled", "false");
            ret.put("dhfs.files.target_chunk_size", "3");
        }
    }
}

public class DhfsFileServiceSimpleTestImpl {
    @Inject
    DhfsFileService fileService;
    @Inject
    Transaction curTx;
    @Inject
    TransactionManager jObjectTxManager;
    @Inject
    RemoteTransaction remoteTx;

//    @Test
//    void readTest() {
//        var fuuid = UUID.randomUUID();
//        {
//            ChunkData c1 = new ChunkData(ByteString.copyFrom("12345".getBytes()));
//            ChunkData c2 = new ChunkData(ByteString.copyFrom("678".getBytes()));
//            ChunkData c3 = new ChunkData(ByteString.copyFrom("91011".getBytes()));
//            File f = new File(fuuid, 777, false);
//            f.chunks().put(0L, c1.getName());
//            f.chunks().put((long) c1.getBytes().size(), c2.getName());
//            f.chunks().put((long) c1.getBytes().size() + c2.getBytes().size(), c3.getName());
//
//            // FIXME: dhfs_files
//
//            var c1o = new AtomicReference<String>();
//            var c2o = new AtomicReference<String>();
//            var c3o = new AtomicReference<String>();
//            var fo = new AtomicReference<String>();
//
//            jObjectTxManager.executeTx(() -> {
//                c1o.set(curTx.put(c1, Optional.of(f.getName())).getMeta().getName());
//                c2o.set(curTx.put(c2, Optional.of(f.getName())).getMeta().getName());
//                c3o.set(curTx.put(c3, Optional.of(f.getName())).getMeta().getName());
//                fo.set(curTx.put(f, Optional.empty()).getMeta().getName());
//            });
//
//            var all = jObjectManager.findAll();
//            Assertions.assertTrue(all.contains(c1o.get()));
//            Assertions.assertTrue(all.contains(c2o.get()));
//            Assertions.assertTrue(all.contains(c3o.get()));
//            Assertions.assertTrue(all.contains(fo.get()));
//        }
//
//        String all = "1234567891011";
//
//        {
//            for (int start = 0; start < all.length(); start++) {
//                for (int end = start; end <= all.length(); end++) {
//                    var read = fileService.read(fuuid.toString(), start, end - start);
//                    Assertions.assertArrayEquals(all.substring(start, end).getBytes(), read.get().toByteArray());
//                }
//            }
//        }
//    }

    @Test
    void dontMkdirTwiceTest() {
        Assertions.assertDoesNotThrow(() -> fileService.mkdir("/dontMkdirTwiceTest", 777));
        Assertions.assertThrows(AlreadyExistsException.class, () -> fileService.mkdir("/dontMkdirTwiceTest", 777));
    }

    @Test
    void writeTest() {
        var ret = fileService.create("/writeTest", 777);
        Assertions.assertTrue(ret.isPresent());

        var uuid = ret.get();

        fileService.write(uuid, 0, new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        Assertions.assertArrayEquals(new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, fileService.read(uuid, 0, 10).get().toByteArray());
        fileService.write(uuid, 4, new byte[]{10, 11, 12});
        Assertions.assertArrayEquals(new byte[]{0, 1, 2, 3, 10, 11, 12, 7, 8, 9}, fileService.read(uuid, 0, 10).get().toByteArray());
        fileService.write(uuid, 10, new byte[]{13, 14});
        Assertions.assertArrayEquals(new byte[]{0, 1, 2, 3, 10, 11, 12, 7, 8, 9, 13, 14}, fileService.read(uuid, 0, 12).get().toByteArray());
        fileService.write(uuid, 6, new byte[]{15, 16});
        Assertions.assertArrayEquals(new byte[]{0, 1, 2, 3, 10, 11, 15, 16, 8, 9, 13, 14}, fileService.read(uuid, 0, 12).get().toByteArray());
        fileService.write(uuid, 3, new byte[]{17, 18});
        Assertions.assertArrayEquals(new byte[]{0, 1, 2, 17, 18, 11, 15, 16, 8, 9, 13, 14}, fileService.read(uuid, 0, 12).get().toByteArray());
    }

    @Test
    void removeTest() {
        var ret = fileService.create("/removeTest", 777);
        Assertions.assertTrue(ret.isPresent());

        var uuid = ret.get();

        fileService.write(uuid, 0, new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        Assertions.assertArrayEquals(new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, fileService.read(uuid, 0, 10).get().toByteArray());

        fileService.unlink("/removeTest");
        Assertions.assertFalse(fileService.open("/removeTest").isPresent());
    }

    @Test
    void truncateTest1() {
        var ret = fileService.create("/truncateTest1", 777);
        Assertions.assertTrue(ret.isPresent());

        var uuid = ret.get();

        fileService.write(uuid, 0, new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        Assertions.assertArrayEquals(new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, fileService.read(uuid, 0, 10).get().toByteArray());

        fileService.truncate(uuid, 20);
        fileService.write(uuid, 5, new byte[]{10, 11, 12, 13, 14, 15, 16, 17});
        Assertions.assertArrayEquals(new byte[]{0, 1, 2, 3, 4, 10, 11, 12, 13, 14, 15, 16, 17, 0, 0, 0, 0, 0, 0, 0}, fileService.read(uuid, 0, 20).get().toByteArray());
    }

    @Test
    void truncateTest2() {
        var ret = fileService.create("/truncateTest2", 777);
        Assertions.assertTrue(ret.isPresent());

        var uuid = ret.get();

        fileService.write(uuid, 0, new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        Assertions.assertArrayEquals(new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, fileService.read(uuid, 0, 10).get().toByteArray());

        fileService.truncate(uuid, 20);
        fileService.write(uuid, 10, new byte[]{11, 12, 13, 14, 15, 16, 17, 18, 19, 20});
        Assertions.assertArrayEquals(new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}, fileService.read(uuid, 0, 20).get().toByteArray());
    }

    @Test
    void truncateTest3() {
        var ret = fileService.create("/truncateTest3", 777);
        Assertions.assertTrue(ret.isPresent());

        var uuid = ret.get();

        fileService.write(uuid, 0, new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        Assertions.assertArrayEquals(new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, fileService.read(uuid, 0, 10).get().toByteArray());

        fileService.truncate(uuid, 7);
        Assertions.assertArrayEquals(new byte[]{0, 1, 2, 3, 4, 5, 6,}, fileService.read(uuid, 0, 20).get().toByteArray());
    }

    @Test
    void moveTest() {
        var ret = fileService.create("/moveTest", 777);
        Assertions.assertTrue(ret.isPresent());
        var uuid = ret.get();

        fileService.write(uuid, 0, new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        Assertions.assertArrayEquals(new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, fileService.read(uuid, 0, 10).get().toByteArray());

        Assertions.assertTrue(fileService.rename("/moveTest", "/movedTest"));
        Assertions.assertFalse(fileService.open("/moveTest").isPresent());
        Assertions.assertTrue(fileService.open("/movedTest").isPresent());

        Assertions.assertArrayEquals(new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
                fileService.read(fileService.open("/movedTest").get(), 0, 10).get().toByteArray());
    }

    @Test
    void moveOverTest() throws InterruptedException {
        var ret = fileService.create("/moveOverTest1", 777);
        Assertions.assertTrue(ret.isPresent());
        var uuid = ret.get();
        var ret2 = fileService.create("/moveOverTest2", 777);
        Assertions.assertTrue(ret2.isPresent());
        var uuid2 = ret2.get();

        fileService.write(uuid, 0, new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        Assertions.assertArrayEquals(new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, fileService.read(uuid, 0, 10).get().toByteArray());
        fileService.write(uuid2, 0, new byte[]{11, 12, 13, 14, 15, 16, 17, 18, 19, 29});
        Assertions.assertArrayEquals(new byte[]{11, 12, 13, 14, 15, 16, 17, 18, 19, 29}, fileService.read(uuid2, 0, 10).get().toByteArray());


        jObjectTxManager.run(() -> {
            var oldfile = remoteTx.getData(File.class, ret2.get()).orElseThrow(IllegalStateException::new);
            var chunk = oldfile.chunks().get(0L);
            var chunkObj = remoteTx.getData(ChunkData.class, chunk).orElseThrow(IllegalStateException::new);
        });

        Assertions.assertTrue(fileService.rename("/moveOverTest1", "/moveOverTest2"));
        Assertions.assertFalse(fileService.open("/moveOverTest1").isPresent());
        Assertions.assertTrue(fileService.open("/moveOverTest2").isPresent());

        Assertions.assertArrayEquals(new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
                fileService.read(fileService.open("/moveOverTest2").get(), 0, 10).get().toByteArray());

//        await().atMost(5, TimeUnit.SECONDS).until(() -> {
//                jObjectTxManager.run(() -> {
//
//                    return chunkObj.runReadLocked(JObjectManager.ResolutionStrategy.LOCAL_ONLY,
//                            (m, d) -> !m.getReferrers().contains(uuid));
//                });
//        });
    }

    @Test
    void readOverSizeTest() {
        var ret = fileService.create("/readOverSizeTest", 777);
        Assertions.assertTrue(ret.isPresent());
        var uuid = ret.get();

        fileService.write(uuid, 0, new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        Assertions.assertArrayEquals(new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, fileService.read(uuid, 0, 10).get().toByteArray());
        Assertions.assertArrayEquals(new byte[]{}, fileService.read(uuid, 20, 10).get().toByteArray());
    }

    @Test
    void writeOverSizeTest() {
        var ret = fileService.create("/writeOverSizeTest", 777);
        Assertions.assertTrue(ret.isPresent());
        var uuid = ret.get();

        fileService.write(uuid, 0, new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        Assertions.assertArrayEquals(new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, fileService.read(uuid, 0, 10).get().toByteArray());
        fileService.write(uuid, 20, new byte[]{10, 11, 12, 13, 14, 15, 16, 17, 18, 19});
        Assertions.assertArrayEquals(new byte[]{
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                10, 11, 12, 13, 14, 15, 16, 17, 18, 19
        }, fileService.read(uuid, 0, 30).get().toByteArray());
    }

    @Test
    void moveTest2() throws InterruptedException {
        var ret = fileService.create("/moveTest2", 777);
        Assertions.assertTrue(ret.isPresent());
        var uuid = ret.get();

        fileService.write(uuid, 0, new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        Assertions.assertArrayEquals(new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, fileService.read(uuid, 0, 10).get().toByteArray());

//        var oldfile = jObjectManager.get(uuid).orElseThrow(IllegalStateException::new);
//        var chunk = oldfile.runReadLocked(JObjectManager.ResolutionStrategy.LOCAL_ONLY, (m, d) -> d.extractRefs()).stream().toList().get(0);
//        var chunkObj = jObjectManager.get(chunk).orElseThrow(IllegalStateException::new);
//
//        chunkObj.runReadLockedVoid(JObjectManager.ResolutionStrategy.LOCAL_ONLY, (m, d) -> {
//            Assertions.assertTrue(m.getReferrers().contains(uuid));
//        });

        Assertions.assertTrue(fileService.rename("/moveTest2", "/movedTest2"));
        Assertions.assertFalse(fileService.open("/moveTest2").isPresent());
        Assertions.assertTrue(fileService.open("/movedTest2").isPresent());

        Assertions.assertArrayEquals(new byte[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
                fileService.read(fileService.open("/movedTest2").get(), 0, 10).get().toByteArray());
    }
}
