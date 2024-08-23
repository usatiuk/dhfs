package com.usatiuk.dhfs.objects.repository.persistence;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.UnsafeByteOperations;
import com.usatiuk.dhfs.SerializationHelper;
import com.usatiuk.dhfs.objects.persistence.JObjectDataP;
import com.usatiuk.dhfs.objects.persistence.ObjectMetadataP;
import com.usatiuk.dhfs.supportlib.UninitializedByteBuffer;
import com.usatiuk.utils.StatusRuntimeExceptionNoStacktrace;
import io.grpc.Status;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.apache.commons.lang3.NotImplementedException;
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
//   64-bit offset of metadata (if 8 then file has no data)
//   data (encoded as JObjectDataP)
//   metadata (encoded as ObjectMetadataP)

@ApplicationScoped
public class FileObjectPersistentStore implements ObjectPersistentStore {
    private final static long mmapThreshold = 65536;
    private final Path _root;
    private final Path _txManifest;
    private ExecutorService _flushExecutor;
    private RandomAccessFile _txFile;

    public FileObjectPersistentStore(@ConfigProperty(name = "dhfs.objects.persistence.files.root") String root) {
        this._root = Path.of(root).resolve("objects");
        _txManifest = Path.of(root).resolve("cur-tx-manifest");
    }

    void init(@Observes @Priority(200) StartupEvent event) throws IOException {
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
    }

    void shutdown(@Observes @Priority(400) ShutdownEvent event) {
        Log.info("Shutdown");
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
        ArrayList<String> out = new ArrayList<>();
        findAllObjectsImpl(out, _root);
        return Collections.unmodifiableCollection(out);
    }

    @Nonnull
    @Override
    public Boolean existsObject(String name) {
        throw new NotImplementedException();
    }

    @Nonnull
    @Override
    public Boolean existsObjectData(String name) {
        throw new NotImplementedException();
    }

    @Nonnull
    @Override
    public JObjectDataP readObject(String name) {
        var path = getObjPath(name);
        try (var rf = new RandomAccessFile(path.toFile(), "r")) {
            var len = rf.length();
            var longBuf = new byte[8];
            rf.readFully(longBuf);

            var metaOff = bytesToLong(longBuf);

            int toRead = (int) (metaOff - 8);

            if (toRead <= 0)
                throw new StatusRuntimeExceptionNoStacktrace(Status.NOT_FOUND);

            ByteBuffer buf;

            //FIXME: rewriting files breaks!
            if (false && len > mmapThreshold) {
                buf = rf.getChannel().map(FileChannel.MapMode.READ_ONLY, 8, toRead);
            } else {
                buf = UninitializedByteBuffer.allocateUninitialized(toRead);
                fillBuffer(buf, rf.getChannel());
                buf.flip();
            }

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
        var path = getObjPath(name);
        try (var rf = new RandomAccessFile(path.toFile(), "r")) {
            var len = rf.length();
            var longBuf = new byte[8];

            rf.readFully(longBuf);

            var metaOff = bytesToLong(longBuf);

            int toRead = (int) (len - metaOff);

            var arr = new byte[(int) toRead];
            rf.seek(metaOff);
            rf.readFully(arr, 0, toRead);
            return ObjectMetadataP.parseFrom(UnsafeByteOperations.unsafeWrap(arr));
        } catch (FileNotFoundException | NoSuchFileException fx) {
            throw new StatusRuntimeExceptionNoStacktrace(Status.NOT_FOUND);
        } catch (IOException e) {
            Log.error("Error reading file " + path, e);
            throw new StatusRuntimeExceptionNoStacktrace(Status.INTERNAL);
        }
    }

    //FIXME: Could be more optimal...
    private byte[] longToBytes(long val) {
        return ByteBuffer.wrap(new byte[8]).putLong(val).array();
    }

    private long bytesToLong(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getLong();
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
            var dataSize = data != null ? data.getSerializedSize() : 0;
            fsb.write(longToBytes(dataSize + 8));

            var totalSize = dataSize + meta.getSerializedSize();
            // Avoids CodedOutputStream flushing all the time
            var bb = UninitializedByteBuffer.allocateUninitialized(totalSize);
            var bbOut = CodedOutputStream.newInstance(bb);

            if (data != null)
                data.writeTo(bbOut);
            meta.writeTo(bbOut);
            bbOut.flush();

            if (fsb.getChannel().write(bb.flip()) != totalSize)
                throw new IOException("Could not write to file");

            if (sync) {
                fsb.flush();
                fsb.getFD().sync();
            }
        }
    }

    private void writeObjectMetaImpl(Path path, ObjectMetadataP meta, boolean sync) throws IOException {
        try (var rf = new RandomAccessFile(path.toFile(), "rw");
             var ch = rf.getChannel()) {
            var longBuf = UninitializedByteBuffer.allocateUninitialized(8);
            fillBuffer(longBuf, ch);

            longBuf.flip();
            var metaOff = longBuf.getLong();

            ch.truncate(metaOff + meta.getSerializedSize());
            ch.position(metaOff);

            var totalSize = meta.getSerializedSize();
            // Avoids CodedOutputStream flushing all the time
            var bb = UninitializedByteBuffer.allocateUninitialized(totalSize);
            var bbOut = CodedOutputStream.newInstance(bb);

            meta.writeTo(bbOut);
            bbOut.flush();
            if (ch.write(bb.flip()) != totalSize)
                throw new IOException("Could not write to file");

            if (sync)
                rf.getFD().sync();
        }
    }

    @Override
    public void writeObjectDirect(String name, ObjectMetadataP meta, JObjectDataP data) {
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
        try {
            var tmpPath = getTmpObjPath(name);
            writeObjectImpl(tmpPath, meta, data, true);
        } catch (IOException e) {
            Log.error("Error writing new file " + name, e);
        }
    }

    @Override
    public void writeNewObjectMeta(String name, ObjectMetadataP meta) {
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

    private void putTransactionManifest(TxManifest manifest) {
        try {
            // TODO: Checksum?
            var channel = _txFile.getChannel();
            var data = SerializationHelper.serializeArray(manifest);
            channel.truncate(data.length);
            channel.position(0);
            if (channel.write(ByteBuffer.wrap(data)) != data.length)
                throw new StatusRuntimeExceptionNoStacktrace(Status.INTERNAL);
            channel.force(true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void commitTx(TxManifest manifest) {
        try {
            putTransactionManifest(manifest);

            var latch = new CountDownLatch(manifest.getWritten().size() + manifest.getDeleted().size());
            ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();

            for (var n : manifest.getWritten()) {
                _flushExecutor.execute(() -> {
                    try {
                        Files.move(getTmpObjPath(n), getObjPath(n), ATOMIC_MOVE, REPLACE_EXISTING);
                    } catch (Throwable t) {
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
    public void deleteObject(String name) {
        deleteImpl(getObjPath(name));
    }

    @Override
    public long getTotalSpace() {
        return _root.toFile().getTotalSpace();
    }

    @Override
    public long getFreeSpace() {
        return _root.toFile().getFreeSpace();
    }

    @Override
    public long getUsableSpace() {
        return _root.toFile().getUsableSpace();
    }

}
