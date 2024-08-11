package com.usatiuk.dhfs.objects.repository.persistence;

import com.google.protobuf.UnsafeByteOperations;
import com.usatiuk.dhfs.objects.persistence.JObjectDataP;
import com.usatiuk.dhfs.objects.persistence.ObjectMetadataP;
import com.usatiuk.utils.StatusRuntimeExceptionNoStacktrace;
import io.grpc.Status;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import org.apache.commons.lang3.NotImplementedException;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.annotation.Nonnull;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

// File format:
//   64-bit offset of metadata (if 8 then file has no data)
//   data (encoded as JObjectDataP)
//   metadata (encoded as ObjectMetadataP)

@ApplicationScoped
public class FileObjectPersistentStore implements ObjectPersistentStore {
    private final static long mmapThreshold = 65536;
    private final Path root;

    public FileObjectPersistentStore(@ConfigProperty(name = "dhfs.objects.persistence.files.root") String root) {
        this.root = Path.of(root).resolve("objects");
    }

    void init(@Observes @Priority(200) StartupEvent event) {
        if (!root.toFile().exists()) {
            Log.info("Initializing with root " + root);
            root.toFile().mkdirs();
            for (int i = 0; i < 256; i++) {
                root.resolve(String.valueOf(i)).toFile().mkdirs();
            }
        }
    }

    void shutdown(@Observes @Priority(400) ShutdownEvent event) {
        Log.info("Shutdown");
    }

    private Path getObjPath(@Nonnull String obj) {
        int h = Objects.hash(obj);
        int p1 = h & 0b00000000_00000000_11111111_00000000;
        return root.resolve(String.valueOf(p1 >> 8)).resolve(obj);
    }

    private void findAllObjectsImpl(Collection<String> out, Path path) {
        var read = path.toFile().listFiles();
        if (read == null) return;

        for (var s : read) {
            if (s.isDirectory()) {
                findAllObjectsImpl(out, s.toPath());
            } else {
                out.add(s.getName());
            }
        }
    }

    @Nonnull
    @Override
    public Collection<String> findAllObjects() {
        ArrayList<String> out = new ArrayList<>();
        findAllObjectsImpl(out, root);
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
            {
                var read = rf.read(longBuf);
                if (read != 8)
                    throw new NotImplementedException();
            }

            var metaOff = bytesToLong(longBuf);

            int toRead = (int) (metaOff - 8);

            if (len > mmapThreshold) {
                var bs = UnsafeByteOperations.unsafeWrap(rf.getChannel().map(FileChannel.MapMode.READ_ONLY, 8, toRead));
                // This way, the input will be considered "immutable" which would allow avoiding copies
                // when parsing byte arrays
                var ch = bs.newCodedInput();
                ch.enableAliasing(true);
                return JObjectDataP.parseFrom(ch);
            } else {
                var arr = new byte[(int) toRead];
                rf.readFully(arr, 0, toRead);
                return JObjectDataP.parseFrom(UnsafeByteOperations.unsafeWrap(arr));
            }
        } catch (FileNotFoundException | NoSuchFileException fx) {
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
            {
                var read = rf.read(longBuf);
                if (read != 8)
                    throw new NotImplementedException();
            }

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

    @Override
    public void writeObject(String name, ObjectMetadataP meta, JObjectDataP data) {
        try {
            var path = getObjPath(name);
            try (var fsb = new FileOutputStream(path.toFile(), false);
                 var buf = new BufferedOutputStream(fsb, Math.min(65536, data.getSerializedSize()))) {
                var dataSize = data.getSerializedSize();
                buf.write(longToBytes(dataSize + 8));
                data.writeTo(buf);
                meta.writeTo(buf);
            }
        } catch (IOException e) {
            Log.error("Error writing file " + name, e);
            throw new StatusRuntimeExceptionNoStacktrace(Status.INTERNAL);
        }
    }

    @Override
    public void writeObjectMeta(String name, ObjectMetadataP meta) {
        try {
            var path = getObjPath(name);
            try (var rf = new RandomAccessFile(path.toFile(), "rw");
                 var ch = rf.getChannel()) {
                var longBuf = ByteBuffer.allocateDirect(8);
                {
                    var read = ch.read(longBuf);
                    if (read != 8)
                        throw new NotImplementedException();
                }

                var metaOff = longBuf.getLong();

                ch.truncate(metaOff + meta.getSerializedSize());
                ch.position(metaOff);

                meta.writeTo(new BufferedOutputStream(Channels.newOutputStream(ch), Math.min(65536, meta.getSerializedSize())));
            }
        } catch (IOException e) {
            Log.error("Error writing file " + name, e);
            throw new StatusRuntimeExceptionNoStacktrace(Status.INTERNAL);
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
        return root.toFile().getTotalSpace();
    }

    @Override
    public long getFreeSpace() {
        return root.toFile().getFreeSpace();
    }

    @Override
    public long getUsableSpace() {
        return root.toFile().getUsableSpace();
    }

}
