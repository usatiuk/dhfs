package com.usatiuk.dhfs.storage.files.service;

import com.usatiuk.dhfs.storage.files.objects.Chunk;
import com.usatiuk.dhfs.storage.files.objects.DirEntry;
import com.usatiuk.dhfs.storage.files.objects.Directory;
import com.usatiuk.dhfs.storage.files.objects.File;
import com.usatiuk.dhfs.storage.objects.repository.ObjectRepository;
import io.quarkus.logging.Log;
import io.quarkus.runtime.Shutdown;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.apache.commons.io.input.ClassLoaderObjectInputStream;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;

// Note: this is not actually reactive
@ApplicationScoped
public class DhfsFileServiceImpl implements DhfsFileService {
    @Inject
    Vertx vertx;
    @Inject
    ObjectRepository objectRepository;

    final static String namespace = "dhfs_files";

    void init(@Observes @Priority(300) StartupEvent event) {
        Log.info("Initializing file service");
        if (!objectRepository.existsObject(namespace, new UUID(0, 0).toString()).await().indefinitely()) {
            objectRepository.createNamespace(namespace).await().indefinitely();
            objectRepository.writeObject(namespace, new UUID(0, 0).toString(),
                    ByteBuffer.wrap(SerializationUtils.serialize(
                            new Directory().setUuid(new UUID(0, 0)))
                    )).await().indefinitely();
        }
        getRoot().await().indefinitely();
    }

    @Shutdown
    void shutdown() {
        Log.info("Shutdown file service");
    }

    // Taken from SerializationUtils
    public static <T> T deserialize(final InputStream inputStream) {
        try (ClassLoaderObjectInputStream in = new ClassLoaderObjectInputStream(Thread.currentThread().getContextClassLoader(), inputStream)) {
            final T obj = (T) in.readObject();
            return obj;
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T deserialize(final byte[] objectData) {
        return deserialize(new ByteArrayInputStream(objectData));
    }

    private Uni<DirEntry> readDirEntry(String uuid) {
        return objectRepository.readObject(namespace, uuid)
                .map(o -> deserialize(o.getData().array()));
    }

    private Uni<Optional<DirEntry>> traverse(DirEntry from, Path path) {
        if (path.getNameCount() == 0) return Uni.createFrom().item(Optional.of(from));
        if (!(from instanceof Directory dir))
            return Uni.createFrom().item(Optional.empty());
        for (var el : dir.getChildren()) {
            if (el.getLeft().equals(path.getName(0).toString())) {
                var ref = readDirEntry(el.getRight().toString()).await().indefinitely();
                if (path.getNameCount() == 1) return Uni.createFrom().item(Optional.of(ref));
                return traverse(ref, path.subpath(1, path.getNameCount()));
            }
        }
        return Uni.createFrom().item(Optional.empty());
    }

    @Override
    public Uni<Optional<DirEntry>> getDirEntry(String name) {
        var root = getRoot().await().indefinitely();
        var found = traverse(root, Path.of(name)).await().indefinitely();
        return Uni.createFrom().item(found);
    }

    @Override
    public Uni<Optional<File>> open(String name) {
        // FIXME:
        var root = getRoot().await().indefinitely();
        var found = traverse(root, Path.of(name)).await().indefinitely();
        if (found.isEmpty()) return Uni.createFrom().item(Optional.empty());
        if (!(found.get() instanceof File)) return Uni.createFrom().item(Optional.empty());
        return Uni.createFrom().item(Optional.of((File) found.get()));
    }

    @Override
    public Uni<Optional<File>> create(String name) {
        // FIXME:
        var root = getRoot().await().indefinitely();
        var found = traverse(root, Path.of(name).getParent()).await().indefinitely();
        if (found.isEmpty()) return Uni.createFrom().item(Optional.empty());
        if (!(found.get() instanceof Directory dir)) return Uni.createFrom().item(Optional.empty());
        var fuuid = UUID.randomUUID();
        File f = new File();
        f.setUuid(fuuid);
        objectRepository.writeObject(namespace, fuuid.toString(), ByteBuffer.wrap(SerializationUtils.serialize(f))).await().indefinitely();
        dir.getChildren().add(Pair.of(Path.of(name).getFileName().toString(), fuuid));
        objectRepository.writeObject(namespace, dir.getUuid().toString(), ByteBuffer.wrap(SerializationUtils.serialize(dir))).await().indefinitely();
        return Uni.createFrom().item(Optional.of((File) f));
    }

    @Override
    public Uni<Iterable<String>> readDir(String name) {
        var root = getRoot().await().indefinitely();
        var found = traverse(root, Path.of(name)).await().indefinitely();
        if (found.isEmpty()) throw new IllegalArgumentException();
        if (!(found.get() instanceof Directory)) throw new IllegalArgumentException();

        var foundDir = (Directory) found.get();
        return Uni.createFrom().item(foundDir.getChildren().stream().map(Pair::getLeft).toList());
    }

    @Override
    public Uni<Optional<byte[]>> read(String fileUuid, long offset, int length) {
        var read = objectRepository.readObject(namespace, fileUuid).map(o -> deserialize(o.getData().array())).await().indefinitely();
        if (!(read instanceof File file)) {
            return Uni.createFrom().item(Optional.empty());
        }

        var chunksAll = file.getChunks();
        var chunks = chunksAll.tailMap(chunksAll.floorKey(offset)).entrySet().iterator();

        ByteBuffer buf = ByteBuffer.allocate(length);

        long curPos = offset;
        var chunk = chunks.next();

        while (curPos < offset + length) {
            var chunkPos = chunk.getKey();

            long offInChunk = curPos - chunkPos;

            long toReadInChunk = (offset + length) - curPos;

            var chunkUuid = chunk.getValue();
            var chunkRead = objectRepository.readObject(namespace, chunkUuid).map(o -> deserialize(o.getData().array())).await().indefinitely();

            if (!(chunkRead instanceof Chunk chunkObj)) {
                Log.error("Chunk requested not a chunk: " + chunkUuid);
                return Uni.createFrom().item(Optional.empty());
            }

            var chunkBytes = chunkObj.getBytes();

            long readableLen = chunkBytes.length - offInChunk;

            var toReadReally = Math.min(readableLen, toReadInChunk);

            buf.put(chunkBytes, (int) offInChunk, (int) toReadReally);

            if (readableLen >= toReadInChunk)
                break;
            else
                curPos += readableLen;

            chunk = chunks.next();
        }

        return Uni.createFrom().item(Optional.of(buf.array()));
    }

    @Override
    public Uni<Long> write(String fileUuid, long offset, byte[] data) {
        var read = objectRepository.readObject(namespace, fileUuid).map(o -> deserialize(o.getData().array())).await().indefinitely();
        if (!(read instanceof File file)) {
            return Uni.createFrom().item(-1L);
        }

        var chunksAll = file.getChunks();

        var first = chunksAll.floorEntry(offset);
        var last = chunksAll.floorEntry((offset + data.length) - 1);

        var newChunks = new TreeMap<Long, String>();
        for (var c : chunksAll.entrySet()) {
            if (c.getKey() < offset) newChunks.put(c.getKey(), c.getValue());
        }

        if (first != null && first.getKey() < offset) {
            var chunkUuid = first.getValue();
            var chunkRead = objectRepository.readObject(namespace, chunkUuid).map(o -> deserialize(o.getData().array())).await().indefinitely();
            if (!(chunkRead instanceof Chunk chunkObj)) {
                Log.error("Chunk requested not a chunk: " + chunkUuid);
                return Uni.createFrom().item(-1L);
            }

            var chunkBytes = chunkObj.getBytes();
            Chunk newChunk = new Chunk(Arrays.copyOfRange(chunkBytes, 0, (int) (offset - first.getKey())));
            objectRepository.writeObject(namespace, newChunk.getHash(), ByteBuffer.wrap(SerializationUtils.serialize(newChunk))).await().indefinitely();

            newChunks.put(first.getKey(), newChunk.getHash());
        }

        {
            Chunk newChunk = new Chunk(data);
            objectRepository.writeObject(namespace, newChunk.getHash(), ByteBuffer.wrap(SerializationUtils.serialize(newChunk))).await().indefinitely();

            newChunks.put(offset, newChunk.getHash());
        }
        if (last != null) {
            var lchunkUuid = last.getValue();
            var lchunkRead = objectRepository.readObject(namespace, lchunkUuid).map(o -> deserialize(o.getData().array())).await().indefinitely();
            if (!(lchunkRead instanceof Chunk lchunkObj)) {
                Log.error("Chunk requested not a chunk: " + lchunkUuid);
                return Uni.createFrom().item(-1L);
            }

            var lchunkBytes = lchunkObj.getBytes();

            if (last.getKey() + lchunkBytes.length > offset + data.length) {
                int start = (int) ((offset + data.length) - last.getKey());
                Chunk newChunk = new Chunk(Arrays.copyOfRange(lchunkBytes, start, lchunkBytes.length - start));
                objectRepository.writeObject(namespace, newChunk.getHash(), ByteBuffer.wrap(SerializationUtils.serialize(newChunk))).await().indefinitely();

                newChunks.put(first.getKey(), newChunk.getHash());
            }
        }

        objectRepository.writeObject(namespace, fileUuid, ByteBuffer.wrap(SerializationUtils.serialize(
                new File().setChunks(newChunks).setUuid(file.getUuid())
        ))).await().indefinitely();

        return Uni.createFrom().item((long) data.length);
    }

    @Override
    public Uni<Boolean> truncate(String fileUuid, long length) {
        var read = objectRepository.readObject(namespace, fileUuid).map(o -> deserialize(o.getData().array())).await().indefinitely();
        if (!(read instanceof File file)) {
            return Uni.createFrom().item(false);
        }

        if (length == 0) {
            objectRepository.writeObject(namespace, fileUuid, ByteBuffer.wrap(SerializationUtils.serialize(
                    new File().setChunks(new TreeMap<>()).setUuid(file.getUuid())
            ))).await().indefinitely();
            return Uni.createFrom().item(true);
        }

        var chunksAll = file.getChunks();

        var newChunks = chunksAll.subMap(0L, length - 1);

        var lastChunk = newChunks.lastEntry();

        if (lastChunk != null) {
            var chunkUuid = lastChunk.getValue();
            var chunkRead = objectRepository.readObject(namespace, chunkUuid).map(o -> deserialize(o.getData().array())).await().indefinitely();
            if (!(chunkRead instanceof Chunk chunkObj)) {
                Log.error("Chunk requested not a chunk: " + chunkUuid);
                return Uni.createFrom().item(false);
            }

            var chunkBytes = chunkObj.getBytes();

            if (lastChunk.getKey() + chunkBytes.length > 0) {
                int start = (int) (length - lastChunk.getKey());
                Chunk newChunk = new Chunk(Arrays.copyOfRange(chunkBytes, 0, (int) (length - start)));
                objectRepository.writeObject(namespace, newChunk.getHash(), ByteBuffer.wrap(SerializationUtils.serialize(newChunk))).await().indefinitely();

                newChunks.put(lastChunk.getKey(), newChunk.getHash());
            }
        }

        objectRepository.writeObject(namespace, fileUuid, ByteBuffer.wrap(SerializationUtils.serialize(
                new File().setChunks(new TreeMap<>(newChunks)).setUuid(file.getUuid())
        ))).await().indefinitely();

        return Uni.createFrom().item(true);
    }

    @Override
    public Uni<Long> size(File f) {
        int size = 0;
        //FIXME:
        for (var chunk : f.getChunks().entrySet()) {
            var lchunkUuid = chunk.getValue();
            var lchunkRead = objectRepository.readObject(namespace, lchunkUuid).map(o -> deserialize(o.getData().array())).await().indefinitely();
            if (!(lchunkRead instanceof Chunk lchunkObj)) {
                Log.error("Chunk requested not a chunk: " + lchunkUuid);
                return Uni.createFrom().item(-1L);
            }

            var lchunkBytes = lchunkObj.getBytes();
            size += lchunkBytes.length;
        }
        return Uni.createFrom().item((long) size);
    }

    @Override
    public Uni<Directory> getRoot() {
        return readDirEntry(new UUID(0, 0).toString()).map(d -> (Directory) d);
    }
}
