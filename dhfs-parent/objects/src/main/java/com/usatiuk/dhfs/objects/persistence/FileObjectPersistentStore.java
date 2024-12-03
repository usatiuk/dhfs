package com.usatiuk.dhfs.objects.persistence;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import com.usatiuk.objects.common.runtime.JObjectKey;
import com.usatiuk.dhfs.supportlib.UninitializedByteBuffer;
import com.usatiuk.dhfs.utils.ByteUtils;
import com.usatiuk.dhfs.utils.SerializationHelper;
import com.usatiuk.dhfs.utils.StatusRuntimeExceptionNoStacktrace;
import io.grpc.Status;
import io.quarkus.arc.properties.IfBuildProperty;
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
import java.util.*;
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
@IfBuildProperty(name = "dhfs.objects.persistence", stringValue = "files")
public class FileObjectPersistentStore implements ObjectPersistentStore {
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

            _flushExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(), factory);
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

    private Path getObjPath(@Nonnull JObjectKey obj) {
        int h = Objects.hash(obj);
        int p1 = h & 0b00000000_00000000_11111111_00000000;
        return _root.resolve(String.valueOf(p1 >> 8)).resolve(obj.toString());
    }

    private Path getTmpObjPath(@Nonnull JObjectKey obj) {
        int h = Objects.hash(obj);
        int p1 = h & 0b00000000_00000000_11111111_00000000;
        return _root.resolve(String.valueOf(p1 >> 8)).resolve(obj + ".tmp");
    }

    private void findAllObjectsImpl(Collection<JObjectKey> out, Path path) {
        var read = path.toFile().listFiles();
        if (read == null) return;

        for (var s : read) {
            if (s.isDirectory()) {
                findAllObjectsImpl(out, s.toPath());
            } else {
                if (s.getName().endsWith(".tmp")) continue; // FIXME:
                out.add(new JObjectKey(s.getName())); // FIXME:
            }
        }
    }

    @Nonnull
    @Override
    public Collection<JObjectKey> findAllObjects() {
        verifyReady();
        ArrayList<JObjectKey> out = new ArrayList<>();
        findAllObjectsImpl(out, _root);
        return Collections.unmodifiableCollection(out);
    }

    @Nonnull
    @Override
    public Optional<ByteString> readObject(JObjectKey name) {
        verifyReady();
        var path = getObjPath(name);
        try (var rf = new RandomAccessFile(path.toFile(), "r")) {
            ByteBuffer buf = UninitializedByteBuffer.allocateUninitialized(Math.toIntExact(rf.getChannel().size()));
            fillBuffer(buf, rf.getChannel());
            buf.flip();

            var bs = UnsafeByteOperations.unsafeWrap(buf);
            // This way, the input will be considered "immutable" which would allow avoiding copies
            // when parsing byte arrays
//            var ch = bs.newCodedInput();
//            ch.enableAliasing(true);
            return Optional.of(bs);
        } catch (EOFException | FileNotFoundException | NoSuchFileException fx) {
            return Optional.empty();
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

    private void writeObjectImpl(Path path, ByteString data, boolean sync) throws IOException {
        try (var fsb = new FileOutputStream(path.toFile(), false)) {
            data.writeTo(fsb);

            if (sync) {
                fsb.flush();
                fsb.getFD().sync();
            }
        }
    }

    @Override
    public void writeObject(JObjectKey name, ByteString obj) {
        verifyReady();
        try {
            var tmpPath = getTmpObjPath(name);
            writeObjectImpl(tmpPath, obj, true);
        } catch (IOException e) {
            Log.error("Error writing new file " + name, e);
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
            if (manifest.getDeleted().isEmpty() && manifest.getWritten().isEmpty()) {
                Log.debug("Empty manifest, skipping");
                return;
            }

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
