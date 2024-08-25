package com.usatiuk.dhfs.objects.repository.persistence;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.UnsafeByteOperations;
import com.usatiuk.dhfs.SerializationHelper;
import com.usatiuk.dhfs.objects.persistence.JObjectDataP;
import com.usatiuk.dhfs.objects.persistence.ObjectMetadataP;
import com.usatiuk.dhfs.supportlib.DhfsSupportNative;
import com.usatiuk.dhfs.supportlib.UninitializedByteBuffer;
import com.usatiuk.utils.ByteUtils;
import com.usatiuk.utils.StatusRuntimeExceptionNoStacktrace;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import net.openhft.hashing.LongHashFunction;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.annotation.Nonnull;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

// File format:
//   64-bit metadata serialized size
//   64-bit offset of "rest of" metadata (if -1 then file has no data,
//      if 0 then file has data and metadata fits into META_BLOCK_SIZE)
//   Until META_BLOCK_SIZE - metadata (encoded as ObjectMetadataP)
//   data (encoded as JObjectDataP)
//   rest of metadata

@ApplicationScoped
public class FileObjectPersistentStore implements ObjectPersistentStore {
    private final int META_BLOCK_SIZE = DhfsSupportNative.PAGE_SIZE;
    private final Path _root;
    private final Path _txManifest;
    private ExecutorService _flushExecutor;
    private RandomAccessFile _txFile;
    private volatile boolean _ready = false;

    public FileObjectPersistentStore(@ConfigProperty(name = "dhfs.objects.persistence.files.root") String root) {
        this._root = Path.of(root).resolve("objects");
        _txManifest = Path.of(root).resolve("cur-tx-manifest");
    }

    void init(@Observes @Priority(100) StartupEvent event) throws IOException {
        if (!_root.toFile().exists()) {
            Log.info("Initializing with root " + _root);
            _root.toFile().mkdirs();
            for (int i = 0; i < 256; i++) {
                _root.resolve(String.valueOf(i)).toFile().mkdirs();
            }
        }
        if (!Files.exists(_txManifest)) {
            Files.createFile(_txManifest);
        }
        _txFile = new RandomAccessFile(_txManifest.toFile(), "rw");
        {
            BasicThreadFactory factory = new BasicThreadFactory.Builder()
                    .namingPattern("persistent-commit-%d")
                    .build();

            _flushExecutor = Executors.newFixedThreadPool(8, factory);
        }

        tryReplay();
        Log.info("Transaction replay done");
        _ready = true;
    }

    void shutdown(@Observes @Priority(900) ShutdownEvent event) throws IOException {
        _ready = false;
        Log.debug("Deleting manifest file");
        _txFile.close();
        Files.delete(_txManifest);
        Log.debug("Manifest file deleted");
    }

    private void verifyReady() {
        if (!_ready) throw new IllegalStateException("Wrong service order!");
    }

    private void tryReplay() {
        var read = readTxManifest();
        if (read != null)
            commitTxImpl(read, false);
    }

    private Path getObjPath(@Nonnull String obj) {
        int h = Objects.hash(obj);
        int p1 = h & 0b00000000_00000000_11111111_00000000;
        return _root.resolve(String.valueOf(p1 >> 8)).resolve(obj);
    }

    private Path getTmpObjPath(@Nonnull String obj) {
        int h = Objects.hash(obj);
        int p1 = h & 0b00000000_00000000_11111111_00000000;
        return _root.resolve(String.valueOf(p1 >> 8)).resolve(obj + ".tmp");
    }

    private void findAllObjectsImpl(Collection<String> out, Path path) {
        var read = path.toFile().listFiles();
        if (read == null) return;

        for (var s : read) {
            if (s.isDirectory()) {
                findAllObjectsImpl(out, s.toPath());
            } else {
                if (s.getName().endsWith(".tmp")) continue; // FIXME:
                out.add(s.getName());
            }
        }
    }

    @Nonnull
    @Override
    public Collection<String> findAllObjects() {
        verifyReady();
        ArrayList<String> out = new ArrayList<>();
        findAllObjectsImpl(out, _root);
        return Collections.unmodifiableCollection(out);
    }

    @Nonnull
    @Override
    public JObjectDataP readObject(String name) {
        verifyReady();
        var path = getObjPath(name);
        try (var rf = new RandomAccessFile(path.toFile(), "r")) {
            var longBuf = new byte[8];
            rf.seek(8);
            rf.readFully(longBuf);
            int metaOff = Math.toIntExact(ByteUtils.bytesToLong(longBuf));

            if (metaOff < 0)
                throw new StatusRuntimeException(Status.NOT_FOUND);

            int toRead;

            if (metaOff > 0)
                toRead = metaOff - META_BLOCK_SIZE;
            else
                toRead = Math.toIntExact(rf.length()) - META_BLOCK_SIZE;

            rf.seek(META_BLOCK_SIZE);

            ByteBuffer buf = UninitializedByteBuffer.allocateUninitialized(toRead);
            fillBuffer(buf, rf.getChannel());
            buf.flip();

            var bs = UnsafeByteOperations.unsafeWrap(buf);
            // This way, the input will be considered "immutable" which would allow avoiding copies
            // when parsing byte arrays
            var ch = bs.newCodedInput();
            ch.enableAliasing(true);
            return JObjectDataP.parseFrom(ch);
        } catch (EOFException | FileNotFoundException | NoSuchFileException fx) {
            throw new StatusRuntimeExceptionNoStacktrace(Status.NOT_FOUND);
        } catch (IOException e) {
            Log.error("Error reading file " + path, e);
            throw new StatusRuntimeExceptionNoStacktrace(Status.INTERNAL);
        }
    }

    @Nonnull
    @Override
    public ObjectMetadataP readObjectMeta(String name) {
        verifyReady();
        var path = getObjPath(name);
        try (var rf = new RandomAccessFile(path.toFile(), "r")) {
            int len = Math.toIntExact(rf.length());
            var buf = UninitializedByteBuffer.allocateUninitialized(META_BLOCK_SIZE);
            fillBuffer(buf, rf.getChannel());

            buf.flip();
            int metaSize = Math.toIntExact(buf.getLong());
            int metaOff = Math.toIntExact(buf.getLong());

            ByteBuffer extraBuf;

            if (metaOff > 0) {
                extraBuf = UninitializedByteBuffer.allocateUninitialized(len - metaOff);
                rf.seek(metaOff);
                fillBuffer(extraBuf, rf.getChannel());
            } else if (metaOff < 0) {
                if (len > META_BLOCK_SIZE) {
                    extraBuf = UninitializedByteBuffer.allocateUninitialized(len - META_BLOCK_SIZE);
                    fillBuffer(extraBuf, rf.getChannel());
                } else {
                    extraBuf = null;
                }
            } else {
                extraBuf = null;
            }

            ByteString bs = UnsafeByteOperations.unsafeWrap(buf.position(16).slice());
            if (extraBuf != null) {
                extraBuf.flip();
                bs = bs.concat(UnsafeByteOperations.unsafeWrap(extraBuf));
            }

            bs = bs.substring(0, metaSize);

            return ObjectMetadataP.parseFrom(bs);
        } catch (FileNotFoundException | NoSuchFileException fx) {
            throw new StatusRuntimeExceptionNoStacktrace(Status.NOT_FOUND);
        } catch (IOException e) {
            Log.error("Error reading file " + path, e);
            throw new StatusRuntimeExceptionNoStacktrace(Status.INTERNAL);
        }
    }

    private void fillBuffer(ByteBuffer dst, FileChannel src) throws IOException {
        int rem = dst.remaining();
        int readTotal = 0;
        int readCur = 0;
        while (readTotal < rem && (readCur = src.read(dst)) != -1) {
            readTotal += readCur;
        }
        if (rem != readTotal)
            throw new EOFException();
    }

    private void writeObjectImpl(Path path, ObjectMetadataP meta, JObjectDataP data, boolean sync) throws IOException {
        try (var fsb = new FileOutputStream(path.toFile(), false)) {
            int metaSize = meta.getSerializedSize() + 16;
            int dataSize = data == null ? 0 : data.getSerializedSize();

            // Avoids CodedOutputStream flushing all the time
            var metaBb = UninitializedByteBuffer.allocateUninitialized(Math.max(META_BLOCK_SIZE, meta.getSerializedSize() + 16));
            metaBb.putLong(metaSize - 16);
            if (data == null)
                metaBb.putLong(-1);
            else if (metaSize <= META_BLOCK_SIZE)
                metaBb.putLong(0);
            else
                metaBb.putLong(META_BLOCK_SIZE + dataSize);
            {
                var metaBbOut = CodedOutputStream.newInstance(metaBb);
                meta.writeTo(metaBbOut);
                metaBbOut.flush();
                metaBb.flip();
            }

            if (fsb.getChannel().write(metaBb.limit(META_BLOCK_SIZE)) != META_BLOCK_SIZE)
                throw new IOException("Could not write to file");

            if (data != null) {
                var dataBb = UninitializedByteBuffer.allocateUninitialized(dataSize);
                var dataBbOut = CodedOutputStream.newInstance(dataBb);
                data.writeTo(dataBbOut);
                dataBbOut.flush();
                dataBb.flip();
                if (fsb.getChannel().write(dataBb) != dataSize)
                    throw new IOException("Could not write to file");
            }

            if (metaSize > META_BLOCK_SIZE) {
                if (fsb.getChannel().write(metaBb.limit(metaSize).position(META_BLOCK_SIZE)) != metaSize - META_BLOCK_SIZE)
                    throw new IOException("Could not write to file");
            }

            if (sync) {
                fsb.flush();
                fsb.getFD().sync();
            }
        }
    }

    private void writeObjectMetaImpl(Path path, ObjectMetadataP meta, boolean sync) throws IOException {
        try (var rf = new RandomAccessFile(path.toFile(), "rw");
             var ch = rf.getChannel()) {
            int len = Math.toIntExact(rf.length());
            var buf = UninitializedByteBuffer.allocateUninitialized(META_BLOCK_SIZE);
            fillBuffer(buf, rf.getChannel());

            buf.flip();
            buf.position(8);
            int metaOff = Math.toIntExact(buf.getLong());

            int metaSize = meta.getSerializedSize() + 16;
            int dataSize;

            if (metaOff > 0) {
                dataSize = metaOff - META_BLOCK_SIZE;
            } else if (metaOff < 0) {
                dataSize = 0;
            } else {
                dataSize = len - META_BLOCK_SIZE;
            }

            ch.truncate(Math.max(metaSize, META_BLOCK_SIZE) + dataSize);
            ch.position(0);

            // Avoids CodedOutputStream flushing all the time
            var metaBb = UninitializedByteBuffer.allocateUninitialized(Math.max(META_BLOCK_SIZE, meta.getSerializedSize() + 16));
            metaBb.putLong(metaSize - 16);
            if (dataSize == 0)
                metaBb.putLong(-1);
            else if (metaSize <= META_BLOCK_SIZE)
                metaBb.putLong(0);
            else
                metaBb.putLong(META_BLOCK_SIZE + dataSize);
            {
                var metaBbOut = CodedOutputStream.newInstance(metaBb);
                meta.writeTo(metaBbOut);
                metaBbOut.flush();
                metaBb.flip();
            }

            if (ch.write(metaBb.limit(META_BLOCK_SIZE)) != META_BLOCK_SIZE)
                throw new IOException("Could not write to file");

            if (metaSize > META_BLOCK_SIZE) {
                ch.position(META_BLOCK_SIZE + dataSize);
                if (ch.write(metaBb.limit(metaSize).position(META_BLOCK_SIZE)) != metaSize - META_BLOCK_SIZE)
                    throw new IOException("Could not write to file");
            }

            if (sync)
                rf.getFD().sync();
        }
    }

    @Override
    public void writeObjectDirect(String name, ObjectMetadataP meta, JObjectDataP data) {
        verifyReady();
        try {
            var path = getObjPath(name);
            writeObjectImpl(path, meta, data, false);
        } catch (IOException e) {
            Log.error("Error writing file " + name, e);
            throw new StatusRuntimeExceptionNoStacktrace(Status.INTERNAL);
        }
    }

    @Override
    public void writeObjectMetaDirect(String name, ObjectMetadataP meta) {
        verifyReady();
        try {
            var path = getObjPath(name);
            writeObjectMetaImpl(path, meta, false);
        } catch (IOException e) {
            Log.error("Error writing file " + name, e);
            throw new StatusRuntimeExceptionNoStacktrace(Status.INTERNAL);
        }
    }

    @Override
    public void writeNewObject(String name, ObjectMetadataP meta, JObjectDataP data) {
        verifyReady();
        try {
            var tmpPath = getTmpObjPath(name);
            writeObjectImpl(tmpPath, meta, data, true);
        } catch (IOException e) {
            Log.error("Error writing new file " + name, e);
        }
    }

    @Override
    public void writeNewObjectMeta(String name, ObjectMetadataP meta) {
        verifyReady();
        // TODO COW
        try {
            var path = getObjPath(name);
            var tmpPath = getTmpObjPath(name);
            if (path.toFile().exists())
                Files.copy(path, getTmpObjPath(name));
            writeObjectMetaImpl(tmpPath, meta, true);
        } catch (IOException e) {
            Log.error("Error writing new file meta " + name, e);
        }
    }

    private TxManifest readTxManifest() {
        try {
            var channel = _txFile.getChannel();

            if (channel.size() == 0)
                return null;

            channel.position(0);

            var buf = ByteBuffer.allocate(Math.toIntExact(channel.size()));

            fillBuffer(buf, channel);
            buf.flip();

            long checksum = buf.getLong();
            var data = buf.slice();
            var hash = LongHashFunction.xx3().hashBytes(data);

            if (hash != checksum)
                throw new StatusRuntimeExceptionNoStacktrace(Status.DATA_LOSS.withDescription("Transaction manifest checksum mismatch!"));

            return SerializationHelper.deserialize(data.array(), data.arrayOffset());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void putTxManifest(TxManifest manifest) {
        try {
            var channel = _txFile.getChannel();
            var data = SerializationHelper.serializeArray(manifest);
            channel.truncate(data.length + 8);
            channel.position(0);
            var hash = LongHashFunction.xx3().hashBytes(data);
            if (channel.write(ByteUtils.longToBb(hash)) != 8)
                throw new StatusRuntimeExceptionNoStacktrace(Status.INTERNAL);
            if (channel.write(ByteBuffer.wrap(data)) != data.length)
                throw new StatusRuntimeExceptionNoStacktrace(Status.INTERNAL);
            channel.force(true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void commitTx(TxManifest manifest) {
        verifyReady();
        commitTxImpl(manifest, true);
    }

    public void commitTxImpl(TxManifest manifest, boolean failIfNotFound) {
        try {
            putTxManifest(manifest);

            var latch = new CountDownLatch(manifest.getWritten().size() + manifest.getDeleted().size());
            ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();

            for (var n : manifest.getWritten()) {
                _flushExecutor.execute(() -> {
                    try {
                        Files.move(getTmpObjPath(n), getObjPath(n), ATOMIC_MOVE, REPLACE_EXISTING);
                    } catch (Throwable t) {
                        if (!failIfNotFound && (t instanceof NoSuchFileException)) return;
                        Log.error("Error writing " + n, t);
                        errors.add(t);
                    } finally {
                        latch.countDown();
                    }
                });
            }
            for (var d : manifest.getDeleted()) {
                _flushExecutor.execute(() -> {
                    try {
                        deleteImpl(getObjPath(d));
                    } catch (Throwable t) {
                        Log.error("Error deleting " + d, t);
                        errors.add(t);
                    } finally {
                        latch.countDown();
                    }
                });
            }

            latch.await();

            if (!errors.isEmpty()) {
                throw new RuntimeException("Errors when commiting tx!");
            }

            // No real need to truncate here
//            try (var channel = _txFile.getChannel()) {
//                channel.truncate(0);
//            }
//        } catch (IOException e) {
//            Log.error("Failed committing transaction to disk: ", e);
//            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private void deleteImpl(Path path) {
        try {
            Files.delete(path);
        } catch (NoSuchFileException ignored) {
        } catch (IOException e) {
            Log.error("Error deleting file " + path, e);
            throw new StatusRuntimeExceptionNoStacktrace(Status.INTERNAL);
        }
    }

    @Override
    public void deleteObjectDirect(String name) {
        verifyReady();
        deleteImpl(getObjPath(name));
    }

    @Override
    public long getTotalSpace() {
        verifyReady();
        return _root.toFile().getTotalSpace();
    }

    @Override
    public long getFreeSpace() {
        verifyReady();
        return _root.toFile().getFreeSpace();
    }

    @Override
    public long getUsableSpace() {
        verifyReady();
        return _root.toFile().getUsableSpace();
    }

}
