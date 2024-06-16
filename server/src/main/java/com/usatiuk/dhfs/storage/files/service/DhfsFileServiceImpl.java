package com.usatiuk.dhfs.storage.files.service;

import com.usatiuk.dhfs.storage.files.objects.*;
import com.usatiuk.dhfs.storage.objects.jrepository.JObjectManager;
import com.usatiuk.dhfs.storage.objects.repository.ObjectRepository;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
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
        if (!objectRepository.existsObject(new UUID(0, 0).toString())) {
            jObjectManager.put(new Directory(new UUID(0, 0), 0755));
        }
        getRoot();
    }

    void shutdown(@Observes @Priority(100) ShutdownEvent event) {
        Log.info("Shutdown");
    }

    private Optional<FsNode> traverse(FsNode from, Path path) {
        if (path.getNameCount() == 0) return Optional.of(from);

        if (!(from instanceof Directory dir))
            return Optional.empty();

        var pathFirstPart = path.getName(0).toString();

        var found = dir.getKid(pathFirstPart);
        if (found.isEmpty())
            return Optional.empty();

        var ref = jObjectManager.get(found.get().toString(), FsNode.class);

        if (ref.isEmpty()) {
            Log.error("File missing when traversing directory " + from.getName() + ": " + found);
            return Optional.empty();
        }

        if (path.getNameCount() == 1) return ref;

        return traverse(ref.get(), path.subpath(1, path.getNameCount()));
    }

    @Override
    public Optional<FsNode> getDirEntry(String name) {
        var root = getRoot();
        var found = traverse(root, Path.of(name));
        return found;
    }

    @Override
    public Optional<File> open(String name) {
        // FIXME:
        var root = getRoot();
        var found = traverse(root, Path.of(name));

        if (found.isEmpty())
            return Optional.empty();

        if (!(found.get() instanceof File))
            return Optional.empty();

        return Optional.of((File) found.get());
    }

    @Override
    public Optional<File> create(String name, long mode) {
        // FIXME:
        var root = getRoot();
        var found = traverse(root, Path.of(name).getParent());
        if (found.isEmpty()) return Optional.empty();

        if (!(found.get() instanceof Directory dir)) return Optional.empty();

        var fuuid = UUID.randomUUID();
        File f = new File(fuuid);
        f.setMode(mode);

        jObjectManager.put(f);

        if (!dir.putKid(Path.of(name).getFileName().toString(), fuuid))
            return Optional.empty();

        jObjectManager.put(dir);

        return Optional.of(f);
    }

    @Override
    public Optional<Directory> mkdir(String name, long mode) {
        // FIXME:
        var root = getRoot();
        var found = traverse(root, Path.of(name).getParent());
        if (found.isEmpty()) return Optional.empty();

        if (!(found.get() instanceof Directory dir)) return Optional.empty();

        var duuid = UUID.randomUUID();
        Directory d = new Directory(duuid);
        d.setMode(mode);

        jObjectManager.put(d);
        if (!dir.putKid(Path.of(name).getFileName().toString(), duuid))
            return Optional.empty();
        jObjectManager.put(dir);

        return Optional.of(d);
    }

    private Boolean rmdent(String name) {
        // FIXME:
        var root = getRoot();
        var found = traverse(root, Path.of(name).getParent());
        if (found.isEmpty()) return false;

        if (!(found.get() instanceof Directory dir)) return false;

        var removed = dir.removeKid(Path.of(name).getFileName().toString());
        if (removed) jObjectManager.put(dir);

        return removed;
    }

    @Override
    public Boolean rmdir(String name) {
        return rmdent(name);
    }

    @Override
    public Boolean unlink(String name) {
        return rmdent(name);
    }

    @Override
    public Boolean rename(String from, String to) {
        var dent = getDirEntry(from);
        if (dent.isEmpty()) return false;
        if (!rmdent(from)) return false;

        // FIXME:
        var root = getRoot();
        var found = traverse(root, Path.of(to).getParent());
        if (found.isEmpty()) return false;

        if (!(found.get() instanceof Directory dir)) return false;

        if (!dir.putKid(Path.of(to).getFileName().toString(), dent.get().getUuid()))
            return false;
        jObjectManager.put(dir);

        return true;
    }

    @Override
    public Boolean chmod(String name, long mode) {
        var dent = getDirEntry(name);
        if (dent.isEmpty()) return false;

        dent.get().setMode(mode);

        jObjectManager.put(dent.get());

        return true;
    }

    @Override
    public Iterable<String> readDir(String name) {
        var root = getRoot();
        var found = traverse(root, Path.of(name));
        if (found.isEmpty()) throw new IllegalArgumentException();
        if (!(found.get() instanceof Directory foundDir)) throw new IllegalArgumentException();

        return foundDir.getChildrenList();
    }

    @Override
    public Optional<byte[]> read(String fileUuid, long offset, int length) {
        var fileOpt = jObjectManager.get(fileUuid, File.class);
        if (fileOpt.isEmpty()) {
            Log.error("File not found when trying to read: " + fileUuid);
            return Optional.empty();
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
            return Optional.empty();
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
            var chunkRead = jObjectManager.get(ChunkData.getNameFromHash(chunkUuid), ChunkData.class);

            if (chunkRead.isEmpty()) {
                Log.error("Chunk requested not found: " + chunkUuid);
                return Optional.empty();
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
        return Optional.of(Arrays.copyOf(buf.array(), (int) (curPos - offset)));
    }

    @Override
    public Long write(String fileUuid, long offset, byte[] data) {
        var fileOpt = jObjectManager.get(fileUuid, File.class);
        if (fileOpt.isEmpty()) {
            Log.error("File not found when trying to read: " + fileUuid);
            return -1L;
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
            return -1L;
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
            var chunkRead = jObjectManager.get(ChunkData.getNameFromHash(chunkUuid), ChunkData.class);

            if (chunkRead.isEmpty()) {
                Log.error("Chunk requested not found: " + chunkUuid);
                return -1L;
            }

            var chunkBytes = chunkRead.get().getBytes();
            ChunkData newChunkData = new ChunkData(Arrays.copyOfRange(chunkBytes, 0, (int) (offset - first.getKey())));
            ChunkInfo newChunkInfo = new ChunkInfo(newChunkData.getHash(), newChunkData.getBytes().length);
            jObjectManager.put(newChunkData);
            jObjectManager.put(newChunkInfo);

            newChunks.put(first.getKey(), newChunkData.getHash());
        }

        {
            ChunkData newChunkData = new ChunkData(data);
            ChunkInfo newChunkInfo = new ChunkInfo(newChunkData.getHash(), newChunkData.getBytes().length);
            jObjectManager.put(newChunkData);
            jObjectManager.put(newChunkInfo);

            newChunks.put(offset, newChunkData.getHash());
        }
        if (last != null) {
            var lchunkUuid = last.getValue();
            var lchunkRead = jObjectManager.get(ChunkData.getNameFromHash(lchunkUuid), ChunkData.class);

            if (lchunkRead.isEmpty()) {
                Log.error("Chunk requested not found: " + lchunkUuid);
                return -1L;
            }

            var lchunkBytes = lchunkRead.get().getBytes();

            if (last.getKey() + lchunkBytes.length > offset + data.length) {
                var startInFile = offset + data.length;
                var startInChunk = startInFile - last.getKey();
                ChunkData newChunkData = new ChunkData(Arrays.copyOfRange(lchunkBytes, (int) startInChunk, lchunkBytes.length));
                ChunkInfo newChunkInfo = new ChunkInfo(newChunkData.getHash(), newChunkData.getBytes().length);
                jObjectManager.put(newChunkData);
                jObjectManager.put(newChunkInfo);

                newChunks.put(startInFile, newChunkData.getHash());
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
            return -1L;
        }

        jObjectManager.put(file);

        return (long) data.length;
    }

    @Override
    public Boolean truncate(String fileUuid, long length) {
        var fileOpt = jObjectManager.get(fileUuid, File.class);
        if (fileOpt.isEmpty()) {
            Log.error("File not found when trying to read: " + fileUuid);
            return false;
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
                return false;
            }
            jObjectManager.put(file);
            return true;
        }

        AtomicReference<TreeMap<Long, String>> chunksAllRef = new AtomicReference<>();

        try {
            file.runReadLocked((fsNodeData, fileData) -> {
                chunksAllRef.set(new TreeMap<>(fileData.getChunks()));
                return null;
            });
        } catch (Exception e) {
            Log.error("Error reading file: " + fileUuid, e);
            return false;
        }

        var chunksAll = chunksAllRef.get();

        var newChunks = chunksAll.subMap(0L, length - 1);

        var lastChunk = newChunks.lastEntry();

        if (lastChunk != null) {
            var chunkUuid = lastChunk.getValue();
            var chunkRead = jObjectManager.get(ChunkData.getNameFromHash(chunkUuid), ChunkData.class);

            if (chunkRead.isEmpty()) {
                Log.error("Chunk requested not found: " + chunkUuid);
                return false;
            }

            var chunkBytes = chunkRead.get().getBytes();

            if (lastChunk.getKey() + chunkBytes.length > 0) {
                int start = (int) (length - lastChunk.getKey());
                ChunkData newChunkData = new ChunkData(Arrays.copyOfRange(chunkBytes, 0, (int) (length - start)));
                ChunkInfo newChunkInfo = new ChunkInfo(newChunkData.getHash(), newChunkData.getBytes().length);
                jObjectManager.put(newChunkData);
                jObjectManager.put(newChunkInfo);

                newChunks.put(lastChunk.getKey(), newChunkData.getHash());
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
            return false;
        }

        jObjectManager.put(file);

        return true;
    }

    @Override
    public Boolean setTimes(String fileUuid, long atimeMs, long mtimeMs) {
        var fileOpt = jObjectManager.get(fileUuid, File.class);
        if (fileOpt.isEmpty()) {
            Log.error("File not found when trying to read: " + fileUuid);
            return false;
        }
        var file = fileOpt.get();

        try {
            file.runWriteLocked((fsNodeData, fileData) -> {
                fsNodeData.setMtime(mtimeMs);
                return null;
            });
        } catch (Exception e) {
            Log.error("Error writing file chunks: " + fileUuid, e);
            return false;
        }

        jObjectManager.put(file);

        return true;
    }

    @Override
    public Long size(File f) {
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
            return -1L;
        }

        var chunksAll = chunksAllRef.get();

        for (var chunk : chunksAll.entrySet()) {
            var chunkUuid = chunk.getValue();
            var chunkRead = jObjectManager.get(ChunkInfo.getNameFromHash(chunkUuid), ChunkInfo.class);

            if (chunkRead.isEmpty()) {
                Log.error("Chunk requested not found: " + chunkUuid);
                return -1L;
            }

            size += chunkRead.get().getSize();
        }
        return (long) size;
    }

    @Override
    public Directory getRoot() {
        var read = jObjectManager.get(new UUID(0, 0).toString(), FsNode.class);
        if (read.isEmpty() || !(read.get() instanceof Directory)) {
            Log.error("Root directory not found");
        }
        return (Directory) read.get();
    }
}
