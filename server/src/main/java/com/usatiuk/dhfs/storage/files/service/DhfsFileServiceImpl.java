package com.usatiuk.dhfs.storage.files.service;

import com.usatiuk.dhfs.storage.files.objects.Chunk;
import com.usatiuk.dhfs.storage.files.objects.Directory;
import com.usatiuk.dhfs.storage.files.objects.File;
import com.usatiuk.dhfs.storage.files.objects.FsNode;
import com.usatiuk.dhfs.storage.objects.jrepository.JObjectManager;
import com.usatiuk.dhfs.storage.objects.repository.ObjectRepository;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.Vertx;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

// Note: this is not actually reactive
@ApplicationScoped
public class DhfsFileServiceImpl implements DhfsFileService {
    @Inject
    Vertx vertx;
    @Inject
    JObjectManager jObjectManager;
    @Inject
    ObjectRepository objectRepository;

    final static String namespace = "dhfs_files";

    void init(@Observes @Priority(500) StartupEvent event) {
        Log.info("Initializing file service");
        if (!objectRepository.existsObject(namespace, new UUID(0, 0).toString()).await().indefinitely()) {
            objectRepository.createNamespace(namespace).await().indefinitely();
            jObjectManager.put(namespace, new Directory(new UUID(0, 0), 0755)).await().indefinitely();
        }
        getRoot().await().indefinitely();
    }

    void shutdown(@Observes @Priority(100) ShutdownEvent event) {
        Log.info("Shutdown");
    }

    private Uni<Optional<FsNode>> traverse(FsNode from, Path path) {
        if (path.getNameCount() == 0) return Uni.createFrom().item(Optional.of(from));

        if (!(from instanceof Directory dir))
            return Uni.createFrom().item(Optional.empty());

        var pathFirstPart = path.getName(0).toString();

        var found = dir.getKid(pathFirstPart);
        if (found.isEmpty())
            return Uni.createFrom().item(Optional.empty());

        var ref = jObjectManager.get(namespace, found.get().toString(), FsNode.class)
                .await().indefinitely();

        if (ref.isEmpty()) {
            Log.error("File missing when traversing directory " + from.getName() + ": " + found);
            return Uni.createFrom().item(Optional.empty());
        }

        if (path.getNameCount() == 1) return Uni.createFrom().item(ref);

        return traverse(ref.get(), path.subpath(1, path.getNameCount()));
    }

    @Override
    public Uni<Optional<FsNode>> getDirEntry(String name) {
        var root = getRoot().await().indefinitely();
        var found = traverse(root, Path.of(name)).await().indefinitely();
        return Uni.createFrom().item(found);
    }

    @Override
    public Uni<Optional<File>> open(String name) {
        // FIXME:
        var root = getRoot().await().indefinitely();
        var found = traverse(root, Path.of(name)).await().indefinitely();

        if (found.isEmpty())
            return Uni.createFrom().item(Optional.empty());

        if (!(found.get() instanceof File))
            return Uni.createFrom().item(Optional.empty());

        return Uni.createFrom().item(Optional.of((File) found.get()));
    }

    @Override
    public Uni<Optional<File>> create(String name, long mode) {
        // FIXME:
        var root = getRoot().await().indefinitely();
        var found = traverse(root, Path.of(name).getParent()).await().indefinitely();
        if (found.isEmpty()) return Uni.createFrom().item(Optional.empty());

        if (!(found.get() instanceof Directory dir)) return Uni.createFrom().item(Optional.empty());

        var fuuid = UUID.randomUUID();
        File f = new File(fuuid);
        f.setMode(mode);

        jObjectManager.put(namespace, f).await().indefinitely();

        if (!dir.putKid(Path.of(name).getFileName().toString(), fuuid))
            return Uni.createFrom().item(Optional.empty());

        jObjectManager.put(namespace, dir).await().indefinitely();

        return Uni.createFrom().item(Optional.of(f));
    }

    @Override
    public Uni<Optional<Directory>> mkdir(String name, long mode) {
        // FIXME:
        var root = getRoot().await().indefinitely();
        var found = traverse(root, Path.of(name).getParent()).await().indefinitely();
        if (found.isEmpty()) return Uni.createFrom().item(Optional.empty());

        if (!(found.get() instanceof Directory dir)) return Uni.createFrom().item(Optional.empty());

        var duuid = UUID.randomUUID();
        Directory d = new Directory(duuid);
        d.setMode(mode);

        jObjectManager.put(namespace, d).await().indefinitely();
        if (!dir.putKid(Path.of(name).getFileName().toString(), duuid))
            return Uni.createFrom().item(Optional.empty());
        jObjectManager.put(namespace, dir).await().indefinitely();

        return Uni.createFrom().item(Optional.of(d));
    }

    private Uni<Boolean> rmdent(String name) {
        // FIXME:
        var root = getRoot().await().indefinitely();
        var found = traverse(root, Path.of(name).getParent()).await().indefinitely();
        if (found.isEmpty()) return Uni.createFrom().item(false);

        if (!(found.get() instanceof Directory dir)) return Uni.createFrom().item(false);

        var removed = dir.removeKid(Path.of(name).getFileName().toString());
        if (removed) jObjectManager.put(namespace, dir).await().indefinitely();

        return Uni.createFrom().item(removed);
    }

    @Override
    public Uni<Boolean> rmdir(String name) {
        return rmdent(name);
    }

    @Override
    public Uni<Boolean> unlink(String name) {
        return rmdent(name);
    }

    @Override
    public Uni<Boolean> rename(String from, String to) {
        var dent = getDirEntry(from).await().indefinitely();
        if (dent.isEmpty()) return Uni.createFrom().item(false);
        if (!rmdent(from).await().indefinitely()) return Uni.createFrom().item(false);

        // FIXME:
        var root = getRoot().await().indefinitely();
        var found = traverse(root, Path.of(to).getParent()).await().indefinitely();
        if (found.isEmpty()) return Uni.createFrom().item(false);

        if (!(found.get() instanceof Directory dir)) return Uni.createFrom().item(false);

        if (!dir.putKid(Path.of(to).getFileName().toString(), dent.get().getUuid()))
            return Uni.createFrom().item(false);
        jObjectManager.put(namespace, dir).await().indefinitely();

        return Uni.createFrom().item(true);
    }

    @Override
    public Uni<Boolean> chmod(String name, long mode) {
        var dent = getDirEntry(name).await().indefinitely();
        if (dent.isEmpty()) return Uni.createFrom().item(false);

        dent.get().setMode(mode);

        jObjectManager.put(namespace, dent.get()).await().indefinitely();

        return Uni.createFrom().item(true);
    }

    @Override
    public Uni<Iterable<String>> readDir(String name) {
        var root = getRoot().await().indefinitely();
        var found = traverse(root, Path.of(name)).await().indefinitely();
        if (found.isEmpty()) throw new IllegalArgumentException();
        if (!(found.get() instanceof Directory foundDir)) throw new IllegalArgumentException();

        return Uni.createFrom().item(foundDir.getChildrenList());
    }

    @Override
    public Uni<Optional<byte[]>> read(String fileUuid, long offset, int length) {
        var fileOpt = jObjectManager.get(namespace, fileUuid, File.class).await().indefinitely();
        if (fileOpt.isEmpty()) {
            Log.error("File not found when trying to read: " + fileUuid);
            return Uni.createFrom().item(Optional.empty());
        }
        var file = fileOpt.get();

        AtomicReference<List<Map.Entry<Long, String>>> chunksList = new AtomicReference<>();

        try {
            file.runReadLocked((fsNodeData, fileData) -> {
                var chunksAll = fileData.getChunks();
                chunksList.set(chunksAll.tailMap(chunksAll.floorKey(offset)).entrySet().stream().toList());
                return null;
            });
        } catch (Exception e) {
            Log.error("Error reading file: " + fileUuid, e);
            return Uni.createFrom().item(Optional.empty());
        }

        var chunks = chunksList.get().iterator();
        ByteBuffer buf = ByteBuffer.allocate(length);

        long curPos = offset;
        var chunk = chunks.next();

        while (curPos < offset + length) {
            var chunkPos = chunk.getKey();

            long offInChunk = curPos - chunkPos;

            long toReadInChunk = (offset + length) - curPos;

            var chunkUuid = chunk.getValue();
            var chunkRead = jObjectManager.get(namespace, chunkUuid, Chunk.class).await().indefinitely();

            if (chunkRead.isEmpty()) {
                Log.error("Chunk requested not found: " + chunkUuid);
                return Uni.createFrom().item(Optional.empty());
            }

            var chunkBytes = chunkRead.get().getBytes();

            long readableLen = chunkBytes.length - offInChunk;

            var toReadReally = Math.min(readableLen, toReadInChunk);

            buf.put(chunkBytes, (int) offInChunk, (int) toReadReally);

            curPos += toReadReally;

            if (readableLen > toReadInChunk)
                break;

            if (!chunks.hasNext()) break;

            chunk = chunks.next();
        }

        // FIXME:
        return Uni.createFrom().item(Optional.of(Arrays.copyOf(buf.array(), (int) (curPos - offset))));
    }

    @Override
    public Uni<Long> write(String fileUuid, long offset, byte[] data) {
        var fileOpt = jObjectManager.get(namespace, fileUuid, File.class).await().indefinitely();
        if (fileOpt.isEmpty()) {
            Log.error("File not found when trying to read: " + fileUuid);
            return Uni.createFrom().item(-1L);
        }
        var file = fileOpt.get();

        AtomicReference<TreeMap<Long, String>> chunksAllRef = new AtomicReference<>();

        // FIXME:
        try {
            file.runReadLocked((fsNodeData, fileData) -> {
                chunksAllRef.set(new TreeMap<>(fileData.getChunks()));
                return null;
            });
        } catch (Exception e) {
            Log.error("Error reading file: " + fileUuid, e);
            return Uni.createFrom().item(-1L);
        }

        var chunksAll = chunksAllRef.get();

        var first = chunksAll.floorEntry(offset);
        var last = chunksAll.floorEntry((offset + data.length) - 1);

        var newChunks = new TreeMap<Long, String>();
        for (var c : chunksAll.entrySet()) {
            if (c.getKey() < offset) newChunks.put(c.getKey(), c.getValue());
        }

        if (first != null && first.getKey() < offset) {
            var chunkUuid = first.getValue();
            var chunkRead = jObjectManager.get(namespace, chunkUuid, Chunk.class).await().indefinitely();

            if (chunkRead.isEmpty()) {
                Log.error("Chunk requested not found: " + chunkUuid);
                return Uni.createFrom().item(-1L);
            }

            var chunkBytes = chunkRead.get().getBytes();
            Chunk newChunk = new Chunk(Arrays.copyOfRange(chunkBytes, 0, (int) (offset - first.getKey())));
            jObjectManager.put(namespace, newChunk).await().indefinitely();

            newChunks.put(first.getKey(), newChunk.getHash());
        }

        {
            Chunk newChunk = new Chunk(data);
            jObjectManager.put(namespace, newChunk).await().indefinitely();

            newChunks.put(offset, newChunk.getHash());
        }
        if (last != null) {
            var lchunkUuid = last.getValue();
            var lchunkRead = jObjectManager.get(namespace, lchunkUuid, Chunk.class).await().indefinitely();

            if (lchunkRead.isEmpty()) {
                Log.error("Chunk requested not found: " + lchunkUuid);
                return Uni.createFrom().item(-1L);
            }

            var lchunkBytes = lchunkRead.get().getBytes();

            if (last.getKey() + lchunkBytes.length > offset + data.length) {
                var startInFile = offset + data.length;
                var startInChunk = startInFile - last.getKey();
                Chunk newChunk = new Chunk(Arrays.copyOfRange(lchunkBytes, (int) startInChunk, lchunkBytes.length));
                jObjectManager.put(namespace, newChunk).await().indefinitely();

                newChunks.put(startInFile, newChunk.getHash());
            }
        }

        try {
            file.runWriteLocked((fsNodeData, fileData) -> {
                fileData.getChunks().clear();
                fileData.getChunks().putAll(newChunks);
                fsNodeData.setMtime(System.currentTimeMillis());
                return null;
            });
        } catch (Exception e) {
            Log.error("Error writing file chunks: " + fileUuid, e);
            return Uni.createFrom().item(-1L);
        }

        jObjectManager.put(namespace, file).await().indefinitely();

        return Uni.createFrom().item((long) data.length);
    }

    @Override
    public Uni<Boolean> truncate(String fileUuid, long length) {
        var fileOpt = jObjectManager.get(namespace, fileUuid, File.class).await().indefinitely();
        if (fileOpt.isEmpty()) {
            Log.error("File not found when trying to read: " + fileUuid);
            return Uni.createFrom().item(false);
        }
        var file = fileOpt.get();

        if (length == 0) {
            try {
                file.runWriteLocked((fsNodeData, fileData) -> {
                    fileData.getChunks().clear();
                    fsNodeData.setMtime(System.currentTimeMillis());
                    return null;
                });
            } catch (Exception e) {
                Log.error("Error writing file chunks: " + fileUuid, e);
                return Uni.createFrom().item(false);
            }
            jObjectManager.put(namespace, file).await().indefinitely();
            return Uni.createFrom().item(true);
        }

        AtomicReference<TreeMap<Long, String>> chunksAllRef = new AtomicReference<>();

        try {
            file.runReadLocked((fsNodeData, fileData) -> {
                chunksAllRef.set(new TreeMap<>(fileData.getChunks()));
                return null;
            });
        } catch (Exception e) {
            Log.error("Error reading file: " + fileUuid, e);
            return Uni.createFrom().item(false);
        }

        var chunksAll = chunksAllRef.get();

        var newChunks = chunksAll.subMap(0L, length - 1);

        var lastChunk = newChunks.lastEntry();

        if (lastChunk != null) {
            var chunkUuid = lastChunk.getValue();
            var chunkRead = jObjectManager.get(namespace, chunkUuid, Chunk.class).await().indefinitely();

            if (chunkRead.isEmpty()) {
                Log.error("Chunk requested not found: " + chunkUuid);
                return Uni.createFrom().item(false);
            }

            var chunkBytes = chunkRead.get().getBytes();

            if (lastChunk.getKey() + chunkBytes.length > 0) {
                int start = (int) (length - lastChunk.getKey());
                Chunk newChunk = new Chunk(Arrays.copyOfRange(chunkBytes, 0, (int) (length - start)));
                jObjectManager.put(namespace, newChunk).await().indefinitely();

                newChunks.put(lastChunk.getKey(), newChunk.getHash());
            }
        }

        try {
            file.runWriteLocked((fsNodeData, fileData) -> {
                fileData.getChunks().clear();
                fileData.getChunks().putAll(newChunks);
                fsNodeData.setMtime(System.currentTimeMillis());
                return null;
            });
        } catch (Exception e) {
            Log.error("Error writing file chunks: " + fileUuid, e);
            return Uni.createFrom().item(false);
        }

        jObjectManager.put(namespace, file).await().indefinitely();

        return Uni.createFrom().item(true);
    }

    @Override
    public Uni<Boolean> setTimes(String fileUuid, long atimeMs, long mtimeMs) {
        var fileOpt = jObjectManager.get(namespace, fileUuid, File.class).await().indefinitely();
        if (fileOpt.isEmpty()) {
            Log.error("File not found when trying to read: " + fileUuid);
            return Uni.createFrom().item(false);
        }
        var file = fileOpt.get();

        try {
            file.runWriteLocked((fsNodeData, fileData) -> {
                fsNodeData.setMtime(mtimeMs);
                return null;
            });
        } catch (Exception e) {
            Log.error("Error writing file chunks: " + fileUuid, e);
            return Uni.createFrom().item(false);
        }

        jObjectManager.put(namespace, file).await().indefinitely();

        return Uni.createFrom().item(true);
    }

    @Override
    public Uni<Long> size(File f) {
        int size = 0;
        //FIXME:
        AtomicReference<TreeMap<Long, String>> chunksAllRef = new AtomicReference<>();

        try {
            f.runReadLocked((fsNodeData, fileData) -> {
                chunksAllRef.set(new TreeMap<>(fileData.getChunks()));
                return null;
            });
        } catch (Exception e) {
            Log.error("Error reading file: " + f.getUuid(), e);
            return Uni.createFrom().item(-1L);
        }

        var chunksAll = chunksAllRef.get();

        for (var chunk : chunksAll.entrySet()) {
            var chunkUuid = chunk.getValue();
            var chunkRead = jObjectManager.get(namespace, chunkUuid, Chunk.class).await().indefinitely();

            if (chunkRead.isEmpty()) {
                Log.error("Chunk requested not found: " + chunkUuid);
                return Uni.createFrom().item(-1L);
            }

            var chunkBytes = chunkRead.get().getBytes();
            size += chunkBytes.length;
        }
        return Uni.createFrom().item((long) size);
    }

    @Override
    public Uni<Directory> getRoot() {
        var read = jObjectManager.get(namespace, new UUID(0, 0).toString(), FsNode.class).await().indefinitely();
        if (read.isEmpty() || !(read.get() instanceof Directory)) {
            Log.error("Root directory not found");
        }
        return Uni.createFrom().item((Directory) read.get());
    }
}
