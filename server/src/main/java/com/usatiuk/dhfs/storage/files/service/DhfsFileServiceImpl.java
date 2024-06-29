package com.usatiuk.dhfs.storage.files.service;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import com.usatiuk.dhfs.storage.files.objects.*;
import com.usatiuk.dhfs.storage.objects.jrepository.JObject;
import com.usatiuk.dhfs.storage.objects.jrepository.JObjectManager;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.logging.Log;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.apache.commons.lang3.NotImplementedException;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

// Note: this is not actually reactive
@ApplicationScoped
public class DhfsFileServiceImpl implements DhfsFileService {
    @Inject
    JObjectManager jObjectManager;

    @ConfigProperty(name = "dhfs.storage.files.target_chunk_size")
    Integer targetChunkSize;

    void init(@Observes @Priority(500) StartupEvent event) {
        Log.info("Initializing file service");
        if (jObjectManager.get(new UUID(0, 0).toString()).isEmpty())
            jObjectManager.put(new Directory(new UUID(0, 0), 0755), Optional.empty());
        getRoot();
    }

    private JObject<? extends FsNode> traverse(JObject<? extends FsNode> from, Path path) {
        if (path.getNameCount() == 0) return from;

        var pathFirstPart = path.getName(0).toString();

        var notFound = new StatusRuntimeException(Status.NOT_FOUND.withDescription("Not found: " + from.getName() + "/" + path));

        var found = from.runReadLocked(JObject.ResolutionStrategy.REMOTE, (m, d) -> {
            if (d instanceof Directory dir)
                return dir.getKid(pathFirstPart);
            return Optional.empty();
        }).orElseThrow(() -> notFound);

        Optional<JObject<?>> ref = jObjectManager.get(found.toString());

        if (ref.isEmpty()) {
            Log.error("File missing when traversing directory " + from.getName() + ": " + found);
            throw notFound;
        }

        ref.get().runReadLocked(JObject.ResolutionStrategy.REMOTE, (m, d) -> {
            if (!(d instanceof FsNode))
                throw new StatusRuntimeException(Status.DATA_LOSS.withDescription("FsNode is not an FsNode: " + m.getName()));
            return null;
        });

        if (path.getNameCount() == 1) {
            ref.get().runReadLocked(JObject.ResolutionStrategy.REMOTE, (m, d) -> {
                if (d instanceof File f) {
                    if (!Objects.equals(f.getParent().toString(), from.getName())) {
                        throw new StatusRuntimeException(Status.DATA_LOSS.withDescription("Parent mismatch for file " + path));
                    }
                }
                return null;
            });
            return (JObject<? extends FsNode>) ref.get();
        }

        return traverse((JObject<? extends FsNode>) ref.get(), path.subpath(1, path.getNameCount()));
    }

    private JObject<? extends FsNode> getDirEntry(String name) {
        return traverse(getRoot(), Path.of(name));
    }

    @Override
    public Optional<FsNode> getattr(String uuid) {
        var ref = jObjectManager.get(uuid);
        if (ref.isEmpty()) return Optional.empty();
        return ref.get().runReadLocked(JObject.ResolutionStrategy.REMOTE, (m, d) -> {
            if (!(d instanceof FsNode))
                throw new StatusRuntimeException(Status.DATA_LOSS.withDescription("FsNode is not an FsNode: " + m.getName()));
            //FIXME:
            return Optional.of((FsNode) d);
        });
    }

    @Override
    public Optional<String> open(String name) {
        try {
            return Optional.ofNullable(traverse(getRoot(), Path.of(name)).getName());
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
                return Optional.empty();
            }
            throw e;
        }
    }

    @Override
    public Optional<String> create(String name, long mode) {
        var parent = traverse(getRoot(), Path.of(name).getParent());

        String fname = Path.of(name).getFileName().toString();

        var fuuid = UUID.randomUUID();
        File f = new File(fuuid, mode, UUID.fromString(parent.getName()));

        if (!parent.runWriteLocked(JObject.ResolutionStrategy.REMOTE, (m, d, bump, invalidate) -> {
            if (!(d instanceof Directory dir))
                return false;

            if (dir.getKid(fname).isPresent())
                return false;

            bump.apply();

            jObjectManager.put(f, Optional.of(dir.getName()));

            dir.setMtime(System.currentTimeMillis());
            return dir.putKid(fname, fuuid);
        }))
            return Optional.empty();

        return Optional.of(f.getName());
    }

    @Override
    public Optional<String> mkdir(String name, long mode) {
        var found = traverse(getRoot(), Path.of(name).getParent());

        String dname = Path.of(name).getFileName().toString();

        var duuid = UUID.randomUUID();
        Directory ndir = new Directory(duuid, mode); //FIXME:

        if (!found.runWriteLocked(JObject.ResolutionStrategy.REMOTE, (m, d, bump, invalidate) -> {
            if (!(d instanceof Directory dir))
                return false;

            if (dir.getKid(dname).isPresent())
                return false;

            bump.apply();

            jObjectManager.put(ndir, Optional.of(dir.getName()));

            dir.setMtime(System.currentTimeMillis());
            return dir.putKid(dname, duuid);
        }))
            return Optional.empty();

        return Optional.of(ndir.getName());
    }

    private Boolean rmdent(String name) {
        var parent = getDirEntry(Path.of(name).getParent().toString());

        Optional<UUID> kidId = parent.runReadLocked(JObject.ResolutionStrategy.REMOTE, (m, d) -> {
            if (!(d instanceof Directory dir))
                return Optional.empty();

            return dir.getKid(Path.of(name).getFileName().toString());
        });

        if (kidId.isEmpty())
            return false;

        var kid = jObjectManager.get(kidId.toString());

        if (kid.isEmpty()) return false;

        return parent.runWriteLocked(JObject.ResolutionStrategy.REMOTE, (m, d, bump, i) -> {
            if (!(d instanceof Directory dir))
                return false;

            String kname = Path.of(name).getFileName().toString();

            if (dir.getKid(kname).isPresent())
                return false;

            kid.get().runWriteLocked(JObject.ResolutionStrategy.REMOTE, (m2, d2, bump2, i2) -> {
                m2.removeRef(m.getName());
                return null;
            });

            bump.apply();
            dir.setMtime(System.currentTimeMillis());
            return dir.removeKid(kname);
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
        var theFile = getDirEntry(from);
        var dentFrom = getDirEntry(Paths.get(from).getParent().toString());
        var dentTo = getDirEntry(Paths.get(to).getParent().toString());

        UUID cleanup = null;

        try {
            JObject.rwLockAll(List.of(dentFrom, dentTo));
            theFile.rwLock();

            if (!dentFrom.tryResolve(JObject.ResolutionStrategy.REMOTE))
                throw new StatusRuntimeException(Status.ABORTED.withDescription(dentFrom.getName() + " could not be resolved"));
            if (!dentTo.tryResolve(JObject.ResolutionStrategy.REMOTE))
                throw new StatusRuntimeException(Status.ABORTED.withDescription(dentTo.getName() + " could not be resolved"));
            if (!theFile.tryResolve(JObject.ResolutionStrategy.REMOTE))
                throw new StatusRuntimeException(Status.ABORTED.withDescription(theFile.getName() + " could not be resolved"));

            if (!(dentFrom.getData() instanceof Directory dentFromD))
                throw new StatusRuntimeException(Status.ABORTED.withDescription(dentFrom.getName() + " is not a directory"));
            if (!(dentTo.getData() instanceof Directory dentToD))
                throw new StatusRuntimeException(Status.ABORTED.withDescription(dentTo.getName() + " is not a directory"));

            if (dentFromD.getKid(Paths.get(from).getFileName().toString()).isEmpty())
                throw new NotImplementedException("Race when moving (missing)");

            FsNode newDent;
            if (theFile.getData() instanceof Directory d) {
                newDent = d;
            } else if (theFile.getData() instanceof File f) {
                var newFile = new File(UUID.randomUUID(), f.getMode(), UUID.fromString(dentTo.getName()));
                newFile.setMtime(f.getMtime());
                newFile.setCtime(f.getCtime());
                newFile.getChunks().putAll(f.getChunks());

                for (var c : newFile.getChunks().values()) {
                    var o = jObjectManager.get(ChunkInfo.getNameFromHash(c))
                            .orElseThrow(() -> new StatusRuntimeException(Status.DATA_LOSS.withDescription("Could not find chunk " + c + " when moving " + from)));
                    o.runWriteLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (m, d, b, i) -> {
                        m.addRef(newFile.getName());
                        return null;
                    });
                }

                theFile.getMeta().removeRef(dentFrom.toString());
                jObjectManager.put(newFile, Optional.of(dentTo.getName()));
                newDent = newFile;
            } else {
                throw new StatusRuntimeException(Status.ABORTED.withDescription(theFile.getName() + " is of unknown type"));
            }

            if (!dentFromD.removeKid(Paths.get(from).getFileName().toString()))
                throw new NotImplementedException("Should not reach here");

            String toFn = Paths.get(to).getFileName().toString();

            if (dentToD.getChildren().containsKey(toFn)) {
                cleanup = dentToD.getChildren().get(toFn);
            }

            dentToD.getChildren().put(toFn, newDent.getUuid());
            dentToD.setMtime(System.currentTimeMillis());

            dentFrom.bumpVer();
            dentFrom.notifyWrite();

            dentTo.bumpVer();
            dentTo.notifyWrite();
        } finally {
            dentFrom.rwUnlock();
            dentTo.rwUnlock();
            theFile.rwUnlock();
        }

        if (cleanup != null) {
            jObjectManager.get(cleanup.toString()).ifPresent(c -> {
                c.runWriteLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (m, d, b, i) -> m.removeRef(dentTo.getName()));
            });
        }

        return true;
    }

    @Override
    public Boolean chmod(String name, long mode) {
        var dent = getDirEntry(name);

        dent.runWriteLocked(JObject.ResolutionStrategy.REMOTE, (m, d, bump, i) -> {
            bump.apply();
            d.setMtime(System.currentTimeMillis());
            d.setMode(mode);
            return null;
        });

        return true;
    }

    @Override
    public Iterable<String> readDir(String name) {
        var found = getDirEntry(name);

        return found.runReadLocked(JObject.ResolutionStrategy.REMOTE, (m, d) -> {
            if (!(d instanceof Directory)) {
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT);
            }
            return ((Directory) d).getChildrenList();
        });
    }

    @Override
    public Optional<ByteString> read(String fileUuid, long offset, int length) {
        var fileOpt = jObjectManager.get(fileUuid);
        if (fileOpt.isEmpty()) {
            Log.error("File not found when trying to read: " + fileUuid);
            return Optional.empty();
        }
        var file = fileOpt.get();

        AtomicReference<List<Map.Entry<Long, String>>> chunksList = new AtomicReference<>();

        try {
            file.runReadLocked(JObject.ResolutionStrategy.REMOTE, (md, fileData) -> {
                if (!(fileData instanceof File)) {
                    throw new StatusRuntimeException(Status.INVALID_ARGUMENT);
                }
                var chunksAll = ((File) fileData).getChunks();
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
            return Optional.of(ByteString.empty());
        }

        var chunks = chunksList.get().iterator();
        ByteString buf = ByteString.empty();

        long curPos = offset;
        var chunk = chunks.next();

        while (curPos < offset + length) {
            var chunkPos = chunk.getKey();

            long offInChunk = curPos - chunkPos;

            long toReadInChunk = (offset + length) - curPos;

            var chunkBytes = readChunk(chunk.getValue());

            long readableLen = chunkBytes.size() - offInChunk;

            var toReadReally = Math.min(readableLen, toReadInChunk);

            buf = buf.concat(chunkBytes.substring((int) offInChunk, (int) (offInChunk + toReadReally)));

            curPos += toReadReally;

            if (readableLen > toReadInChunk)
                break;

            if (!chunks.hasNext()) break;

            chunk = chunks.next();
        }

        // FIXME:
        return Optional.of(buf);
    }

    private Integer getChunkSize(String uuid) {
        var chunkRead = jObjectManager.get(ChunkInfo.getNameFromHash(uuid));

        if (chunkRead.isEmpty()) {
            Log.error("Chunk requested not found: " + uuid);
            throw new StatusRuntimeException(Status.NOT_FOUND);
        }

        return chunkRead.get().runReadLocked(JObject.ResolutionStrategy.REMOTE, (m, d) -> {
            if (!(d instanceof ChunkInfo))
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT);
            return ((ChunkInfo) d).getSize();
        });
    }

    private ByteString readChunk(String uuid) {
        var chunkRead = jObjectManager.get(ChunkData.getNameFromHash(uuid));

        if (chunkRead.isEmpty()) {
            Log.error("Chunk requested not found: " + uuid);
            throw new StatusRuntimeException(Status.NOT_FOUND);
        }

        return chunkRead.get().runReadLocked(JObject.ResolutionStrategy.REMOTE, (m, d) -> {
            if (!(d instanceof ChunkData))
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT);
            return ((ChunkData) d).getBytes();
        });
    }

    private void cleanupChunks(String fileUuid, Collection<String> uuids) {
        for (var cuuid : uuids) {
            var ci = jObjectManager.get(ChunkInfo.getNameFromHash(cuuid));
            if (ci.isPresent()) {
                ci.get().runWriteLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (m, d, b, v) -> {
                    m.removeRef(fileUuid);
                    return null;
                });
                jObjectManager.tryQuickDelete(ci.get());
            }
        }
    }

    @Override
    public Long write(String fileUuid, long offset, byte[] data) {
        var fileOpt = jObjectManager.get(fileUuid);
        if (fileOpt.isEmpty()) {
            Log.error("File not found when trying to read: " + fileUuid);
            return -1L;
        }
        var file = fileOpt.get();

        // FIXME:
        var removedChunksOuter = file.runWriteLocked(JObject.ResolutionStrategy.REMOTE, (meta, fDataU, bump, i) -> {
            if (!(fDataU instanceof File))
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT);

            var fData = (File) fDataU;
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

            ByteString pendingWrites = ByteString.empty();

            if (first != null && first.getKey() < offset) {
                var chunkBytes = readChunk(first.getValue());
                pendingWrites = pendingWrites.concat(chunkBytes.substring(0, (int) (offset - first.getKey())));
            }
            pendingWrites = pendingWrites.concat(UnsafeByteOperations.unsafeWrap(data));

            if (last != null) {
                var lchunkBytes = readChunk(last.getValue());
                if (last.getKey() + lchunkBytes.size() > offset + data.length) {
                    var startInFile = offset + data.length;
                    var startInChunk = startInFile - last.getKey();
                    pendingWrites = pendingWrites.concat(lchunkBytes.substring((int) startInChunk, lchunkBytes.size()));
                }
            }

            int combinedSize = pendingWrites.size();

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
                            pendingWrites = pendingWrites.concat(readChunk(cuuid));
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
                            pendingWrites = readChunk(cuuid).concat(pendingWrites);
                            combinedSize += getChunkSize(cuuid);
                            chunksAll.remove(takeRight.getKey());
                            removedChunks.add(cuuid);
                        }
                    }
                }
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

                    var thisChunk = pendingWrites.substring(cur, end);

                    ChunkData newChunkData = new ChunkData(thisChunk);
                    ChunkInfo newChunkInfo = new ChunkInfo(newChunkData.getHash(), newChunkData.getBytes().size());
                    //FIXME:
                    jObjectManager.put(newChunkData, Optional.of(newChunkInfo.getName()));
                    jObjectManager.put(newChunkInfo, Optional.of(meta.getName()));
                    jObjectManager.put(newChunkData, Optional.of(newChunkInfo.getName()));
                    chunksAll.put(start, newChunkInfo.getHash());

                    start += thisChunk.size();
                    cur = end;
                }
            }

            bump.apply();
            fData.setMtime(System.currentTimeMillis());
            return removedChunks;
        });

        cleanupChunks(fileUuid, removedChunksOuter);

        return (long) data.length;
    }

    @Override
    public Boolean truncate(String fileUuid, long length) {
        var fileOpt = jObjectManager.get(fileUuid);
        if (fileOpt.isEmpty()) {
            Log.error("File not found when trying to read: " + fileUuid);
            return false;
        }
        var file = fileOpt.get();

        TreeSet<String> removedChunks = new TreeSet<>();

        if (length == 0) {
            try {
                file.runWriteLocked(JObject.ResolutionStrategy.REMOTE, (m, fileData, bump, i) -> {
                    if (!(fileData instanceof File f))
                        throw new StatusRuntimeException(Status.INVALID_ARGUMENT);
                    bump.apply();
                    removedChunks.addAll(f.getChunks().values());
                    f.getChunks().clear();
                    f.setMtime(System.currentTimeMillis());
                    return null;
                });
            } catch (Exception e) {
                Log.error("Error writing file chunks: " + fileUuid, e);
                return false;
            }
            cleanupChunks(fileUuid, removedChunks);
            return true;
        }

        try {
            file.runWriteLocked(JObject.ResolutionStrategy.REMOTE, (m, fDataU, bump, i) -> {
                if (!(fDataU instanceof File fData))
                    throw new StatusRuntimeException(Status.INVALID_ARGUMENT);
                var chunksAll = fData.getChunks();
                var lastChunk = chunksAll.lastEntry();

                //FIXME!

                if (lastChunk != null) {
                    var size = getChunkSize(lastChunk.getValue());
                    var chunkEnd = size + lastChunk.getKey();

                    if (chunkEnd == length) return null;

                    if (chunkEnd > length) {
                        var chunkData = readChunk(lastChunk.getValue());

                        ChunkData newChunkData = new ChunkData(chunkData.substring(0, (int) (length - lastChunk.getKey())));
                        ChunkInfo newChunkInfo = new ChunkInfo(newChunkData.getHash(), newChunkData.getBytes().size());
                        jObjectManager.put(newChunkData, Optional.of(newChunkInfo.getName()));
                        jObjectManager.put(newChunkInfo, Optional.of(m.getName()));
                        jObjectManager.put(newChunkData, Optional.of(newChunkInfo.getName()));

                        removedChunks.add(lastChunk.getValue());

                        chunksAll.put(lastChunk.getKey(), newChunkData.getHash());
                    } else {
                        write(fileUuid, chunkEnd, new byte[(int) (length - chunkEnd)]);
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
        cleanupChunks(fileUuid, removedChunks);
        return true;
    }

    @Override
    public Boolean setTimes(String fileUuid, long atimeMs, long mtimeMs) {
        var file = jObjectManager.get(fileUuid).orElseThrow(
                () -> new StatusRuntimeException(Status.NOT_FOUND.withDescription(
                        "File not found for setTimes: " + fileUuid))
        );

        file.runWriteLocked(JObject.ResolutionStrategy.REMOTE, (m, fileData, bump, i) -> {
            if (!(fileData instanceof FsNode fd))
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT);

            bump.apply();
            fd.setMtime(mtimeMs);
            return null;
        });

        return true;
    }

    @Override
    public Long size(String uuid) {
        int size = 0;

        NavigableMap<Long, String> chunksAll;

        var read = jObjectManager.get(uuid)
                .orElseThrow(() -> new StatusRuntimeException(Status.NOT_FOUND));

        try {
            chunksAll = read.runReadLocked(JObject.ResolutionStrategy.REMOTE, (fsNodeData, fileData) -> {
                if (!(fileData instanceof File fd))
                    throw new StatusRuntimeException(Status.INVALID_ARGUMENT);
                return new TreeMap<>(fd.getChunks());
            });
        } catch (Exception e) {
            Log.error("Error reading file: " + uuid, e);
            return -1L;
        }

        for (var chunk : chunksAll.entrySet()) {
            size += getChunkSize(chunk.getValue());
        }

        return (long) size;
    }

    private JObject<Directory> getRoot() {
        var read = jObjectManager.get(new UUID(0, 0).toString());
        if (read.isEmpty()) {
            Log.error("Root directory not found");
        }
        return (JObject<Directory>) read.get();
    }
}
