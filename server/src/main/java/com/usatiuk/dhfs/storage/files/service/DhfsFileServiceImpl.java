package com.usatiuk.dhfs.storage.files.service;

import com.usatiuk.dhfs.storage.files.objects.*;
import com.usatiuk.dhfs.storage.objects.jrepository.JObject;
import com.usatiuk.dhfs.storage.objects.jrepository.JObjectManager;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.vertx.mutiny.core.Vertx;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;

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

    @ConfigProperty(name = "dhfs.storage.files.target_chunk_size")
    Integer targetChunkSize;

    final static String namespace = "dhfs_files";

    void init(@Observes @Priority(500) StartupEvent event) {
        Log.info("Initializing file service");
        jObjectManager.getOrPut(new UUID(0, 0).toString(), new Directory(new UUID(0, 0), 0755));
        getRoot();
    }

    void shutdown(@Observes @Priority(100) ShutdownEvent event) {
        Log.info("Shutdown");
    }

    private Optional<JObject<? extends FsNode>> traverse(JObject<? extends FsNode> from, Path path) {
        if (path.getNameCount() == 0) return Optional.of(from);

        if (!(from.isOf(Directory.class)))
            return Optional.empty();

        var pathFirstPart = path.getName(0).toString();

        return ((JObject<Directory>) from).runReadLocked((m, d) -> {
            var found = d.getKid(pathFirstPart);
            if (found.isEmpty())
                return Optional.empty();
            Optional<JObject<? extends FsNode>> ref = jObjectManager.get(found.get().toString(), FsNode.class);

            if (ref.isEmpty()) {
                Log.error("File missing when traversing directory " + from.getName() + ": " + found);
                return Optional.empty();
            }

            if (path.getNameCount() == 1) return ref;

            return traverse(ref.get(), path.subpath(1, path.getNameCount()));
        });
    }

    private Optional<JObject<? extends FsNode>> getDirEntry(String name) {
        var root = getRoot();
        var found = traverse(root, Path.of(name));
        return found;
    }

    @Override
    public Optional<FsNode> getattr(String uuid) {
        Optional<JObject<? extends FsNode>> ref = jObjectManager.get(uuid, FsNode.class);
        if (ref.isEmpty()) return Optional.empty();
        return ref.get().runReadLocked((m, d) -> {
            //FIXME:
            return Optional.of(d);
        });
    }

    @Override
    public Optional<String> open(String name) {
        // FIXME:
        var root = getRoot();
        var found = traverse(root, Path.of(name));

        if (found.isEmpty())
            return Optional.empty();

        return Optional.of(found.get().getName());
    }

    @Override
    public Optional<String> create(String name, long mode) {
        // FIXME:
        var root = getRoot();
        var found = traverse(root, Path.of(name).getParent());
        if (found.isEmpty()) return Optional.empty();

        if (!(found.get().isOf(Directory.class))) return Optional.empty();

        var dir = (JObject<Directory>) found.get();
        var fuuid = UUID.randomUUID();
        File f = new File(fuuid);
        f.setMode(mode);

        jObjectManager.put(f);

        if (!dir.runWriteLocked((m, d, bump) -> {
            bump.apply();
            d.setMtime(System.currentTimeMillis());
            return d.putKid(Path.of(name).getFileName().toString(), fuuid);
        }))
            return Optional.empty();

        return Optional.of(f.getName());
    }

    @Override
    public Optional<String> mkdir(String name, long mode) {
        // FIXME:
        var root = getRoot();
        var found = traverse(root, Path.of(name).getParent());
        if (found.isEmpty()) return Optional.empty();

        if (!(found.get().isOf(Directory.class))) return Optional.empty();

        var duuid = UUID.randomUUID();
        Directory d = new Directory(duuid);
        d.setMode(mode);
        var dir = (JObject<Directory>) found.get();

        jObjectManager.put(d);
        if (!dir.runWriteLocked((m, dd, bump) -> {
            bump.apply();
            d.setMtime(System.currentTimeMillis());
            return dd.putKid(Path.of(name).getFileName().toString(), duuid);
        }))
            return Optional.empty();

        return Optional.of(d.getName());
    }

    private Boolean rmdent(String name) {
        // FIXME:
        var root = getRoot();
        var found = traverse(root, Path.of(name).getParent());
        if (found.isEmpty()) return false;

        if (!(found.get().isOf(Directory.class))) return false;

        var dir = (JObject<Directory>) found.get();
        return dir.runWriteLocked((m, d, bump) -> {
            bump.apply();
            d.setMtime(System.currentTimeMillis());
            return d.removeKid(Path.of(name).getFileName().toString());
        });
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

        if (!(found.get().isOf(Directory.class))) return false;

        var dir = (JObject<Directory>) found.get();

        dir.runWriteLocked((m, d, bump) -> {
            bump.apply();
            d.setMtime(System.currentTimeMillis());
            d.getChildren().put(Path.of(to).getFileName().toString(), UUID.fromString(dent.get().getName()));
            return null;
        });

        return true;
    }

    @Override
    public Boolean chmod(String name, long mode) {
        var dent = getDirEntry(name);
        if (dent.isEmpty()) return false;

        dent.get().runWriteLocked((m, d, bump) -> {
            bump.apply();
            d.setMtime(System.currentTimeMillis());
            d.setMode(mode);
            return null;
        });

        return true;
    }

    @Override
    public Iterable<String> readDir(String name) {
        var root = getRoot();
        var found = traverse(root, Path.of(name));
        if (found.isEmpty()) throw new IllegalArgumentException();
        if (!(found.get().isOf(Directory.class))) throw new IllegalArgumentException();
        var dir = (JObject<Directory>) found.get();

        return dir.runReadLocked((m, d) -> d.getChildrenList());
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
            file.runReadLocked((md, fileData) -> {
                var chunksAll = fileData.getChunks();
                if (chunksAll.isEmpty()) {
                    chunksList.set(new ArrayList<>());
                    return null;
                }
                chunksList.set(chunksAll.tailMap(chunksAll.floorKey(offset)).entrySet().stream().toList());
                return null;
            });
        } catch (Exception e) {
            Log.error("Error reading file: " + fileUuid, e);
            return Optional.empty();
        }

        if (chunksList.get().isEmpty()) {
            return Optional.of(new byte[0]);
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

            var chunkBytes = chunkRead.get().runReadLocked((m, d) -> d.getBytes());

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

    private Integer getChunkSize(String uuid) {
        var chunkRead = jObjectManager.get(ChunkInfo.getNameFromHash(uuid), ChunkInfo.class);

        if (chunkRead.isEmpty()) {
            Log.error("Chunk requested not found: " + uuid);
            throw new StatusRuntimeException(Status.NOT_FOUND);
        }

        return chunkRead.get().runReadLocked((m, d) -> d.getSize());
    }

    private byte[] readChunk(String uuid) {
        var chunkRead = jObjectManager.get(ChunkData.getNameFromHash(uuid), ChunkData.class);

        if (chunkRead.isEmpty()) {
            Log.error("Chunk requested not found: " + uuid);
            throw new StatusRuntimeException(Status.NOT_FOUND);
        }

        return chunkRead.get().runReadLocked((m, d) -> d.getBytes());
    }

    @Override
    public Long write(String fileUuid, long offset, byte[] data) {
        var fileOpt = jObjectManager.get(fileUuid, File.class);
        if (fileOpt.isEmpty()) {
            Log.error("File not found when trying to read: " + fileUuid);
            return -1L;
        }
        var file = fileOpt.get();

        // FIXME:
        var removedChunksOuter = file.runWriteLocked((meta, fData, bump) -> {
            var chunksAll = fData.getChunks();
            var first = chunksAll.floorEntry(offset);
            var last = chunksAll.floorEntry((offset + data.length) - 1);

            TreeSet<String> removedChunks = new TreeSet<>();

            long start = 0;

            if (!chunksAll.isEmpty()) {
                var between = chunksAll.subMap(first.getKey(), true, last.getKey(), true);
                removedChunks.addAll(between.values());
                start = first.getKey();
                between.clear();
            }

            NavigableMap<Long, String> beforeFirst = first != null ? chunksAll.headMap(first.getKey(), false) : Collections.emptyNavigableMap();
            NavigableMap<Long, String> afterLast = last != null ? chunksAll.tailMap(last.getKey(), false) : Collections.emptyNavigableMap();

            List<byte[]> pendingWrites = new LinkedList<>();
            int combinedSize = 0;

            if (first != null && first.getKey() < offset) {
                var chunkBytes = readChunk(first.getValue());
                pendingWrites.addLast(Arrays.copyOfRange(chunkBytes, 0, (int) (offset - first.getKey())));
                combinedSize += pendingWrites.getLast().length;
            }
            pendingWrites.addLast(data);
            combinedSize += pendingWrites.getLast().length;

            if (last != null) {
                var lchunkBytes = readChunk(last.getValue());
                if (last.getKey() + lchunkBytes.length > offset + data.length) {
                    var startInFile = offset + data.length;
                    var startInChunk = startInFile - last.getKey();
                    pendingWrites.addLast(Arrays.copyOfRange(lchunkBytes, (int) startInChunk, lchunkBytes.length));
                    combinedSize += pendingWrites.getLast().length;
                }
            }

            if (Math.abs(combinedSize - targetChunkSize) > targetChunkSize * 0.1) {
                if (combinedSize < targetChunkSize) {
                    boolean leftDone = false;
                    boolean rightDone = false;
                    while (!leftDone && !rightDone) {
                        if (beforeFirst.isEmpty()) leftDone = true;
                        if (!beforeFirst.isEmpty() && !leftDone) {
                            var takeLeft = beforeFirst.lastEntry();

                            var cuuid = takeLeft.getValue();

                            if ((combinedSize + getChunkSize(cuuid)) > (targetChunkSize * 1.2)) {
                                leftDone = true;
                                continue;
                            }

                            beforeFirst.pollLastEntry();
                            start = takeLeft.getKey();
                            pendingWrites.addFirst(readChunk(cuuid));
                            combinedSize += getChunkSize(cuuid);
                            chunksAll.remove(takeLeft.getKey());
                            removedChunks.add(cuuid);
                        }
                        if (afterLast.isEmpty()) rightDone = true;
                        if (!afterLast.isEmpty() && !rightDone) {
                            var takeRight = afterLast.firstEntry();

                            var cuuid = takeRight.getValue();

                            if ((combinedSize + getChunkSize(cuuid)) > (targetChunkSize * 1.2)) {
                                rightDone = true;
                                continue;
                            }

                            afterLast.pollFirstEntry();
                            pendingWrites.addLast(readChunk(cuuid));
                            combinedSize += getChunkSize(cuuid);
                            chunksAll.remove(takeRight.getKey());
                            removedChunks.add(cuuid);
                        }
                    }
                }
            }

            // FIXME:!

            byte[] realbytes = new byte[combinedSize];
            {
                int cur = 0;
                for (var b : pendingWrites) {
                    System.arraycopy(b, 0, realbytes, cur, b.length);
                    cur += b.length;
                }
                pendingWrites.clear();
            }

            {
                int cur = 0;
                while (cur < combinedSize) {
                    int end;
                    if ((combinedSize - cur) > (targetChunkSize * 1.5)) {
                        end = cur + targetChunkSize;
                    } else {
                        end = combinedSize;
                    }

                    byte[] thisChunk = new byte[end - cur];
                    System.arraycopy(realbytes, cur, thisChunk, 0, thisChunk.length);

                    ChunkData newChunkData = new ChunkData(thisChunk);
                    ChunkInfo newChunkInfo = new ChunkInfo(newChunkData.getHash(), newChunkData.getBytes().length);
                    jObjectManager.put(newChunkData);
                    jObjectManager.put(newChunkInfo);
                    chunksAll.put(start, newChunkInfo.getHash());

                    start += thisChunk.length;
                    cur = end;
                }
            }

            bump.apply();
            fData.setMtime(System.currentTimeMillis());
            return removedChunks;
        });

        for (var v : removedChunksOuter) {
            var ci = jObjectManager.get(ChunkInfo.getNameFromHash(v), ChunkInfo.class);
            if (ci.isPresent())
                jObjectManager.unref(ci.get());
            var cd = jObjectManager.get(ChunkData.getNameFromHash(v), ChunkData.class);
            if (cd.isPresent())
                jObjectManager.unref(cd.get());
        }

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
                file.runWriteLocked((m, fileData, bump) -> {
                    bump.apply();
                    fileData.getChunks().clear();
                    fileData.setMtime(System.currentTimeMillis());
                    return null;
                });
            } catch (Exception e) {
                Log.error("Error writing file chunks: " + fileUuid, e);
                return false;
            }
            return true;
        }

        try {
            file.runWriteLocked((m, fData, bump) -> {
                var chunksAll = fData.getChunks();
                var newChunks = chunksAll.subMap(0L, length - 1);

                var lastChunk = newChunks.lastEntry();

                if (lastChunk != null) {
                    var chunkUuid = lastChunk.getValue();
                    var chunkRead = jObjectManager.get(ChunkData.getNameFromHash(chunkUuid), ChunkData.class);

                    if (chunkRead.isEmpty()) {
                        Log.error("Chunk requested not found: " + chunkUuid);
                        return false;
                    }

                    var chunkBytes = chunkRead.get().runReadLocked((m2, d) -> d.getBytes());

                    if (lastChunk.getKey() + chunkBytes.length > 0) {
                        int start = (int) (length - lastChunk.getKey());
                        ChunkData newChunkData = new ChunkData(Arrays.copyOfRange(chunkBytes, 0, (int) (length - start)));
                        ChunkInfo newChunkInfo = new ChunkInfo(newChunkData.getHash(), newChunkData.getBytes().length);
                        jObjectManager.put(newChunkData);
                        jObjectManager.put(newChunkInfo);

                        newChunks.put(lastChunk.getKey(), newChunkData.getHash());
                    }
                }

                bump.apply();
                fData.setMtime(System.currentTimeMillis());

                return null;
            });
        } catch (Exception e) {
            Log.error("Error reading file: " + fileUuid, e);
            return false;
        }

        return true;
    }

    @Override
    public Boolean setTimes(String fileUuid, long atimeMs, long mtimeMs) {
        var fileOpt = jObjectManager.get(fileUuid, FsNode.class);
        if (fileOpt.isEmpty()) {
            Log.error("File not found when trying to read: " + fileUuid);
            return false;
        }
        var file = fileOpt.get();

        try {
            file.runWriteLocked((m, fileData, bump) -> {
                bump.apply();
                fileData.setMtime(mtimeMs);
                return null;
            });
        } catch (Exception e) {
            Log.error("Error writing file chunks: " + fileUuid, e);
            return false;
        }

        return true;
    }

    @Override
    public Long size(String uuid) {
        int size = 0;
        //FIXME:
        AtomicReference<TreeMap<Long, String>> chunksAllRef = new AtomicReference<>();
        var read = jObjectManager.get(uuid, File.class)
                .orElseThrow(() -> new StatusRuntimeException(Status.NOT_FOUND));

        try {
            read.runReadLocked((fsNodeData, fileData) -> {
                chunksAllRef.set(new TreeMap<>(fileData.getChunks()));
                return null;
            });
        } catch (Exception e) {
            Log.error("Error reading file: " + uuid, e);
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

            size += chunkRead.get().runReadLocked((m, d) -> d.getSize());
        }
        return (long) size;
    }

    private JObject<Directory> getRoot() {
        var read = jObjectManager.get(new UUID(0, 0).toString(), Directory.class);
        if (read.isEmpty()) {
            Log.error("Root directory not found");
        }
        return (JObject<Directory>) read.get();
    }
}
