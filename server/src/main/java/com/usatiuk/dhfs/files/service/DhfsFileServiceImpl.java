package com.usatiuk.dhfs.files.service;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import com.usatiuk.dhfs.files.objects.*;
import com.usatiuk.dhfs.objects.jrepository.JObject;
import com.usatiuk.dhfs.objects.jrepository.JObjectManager;
import com.usatiuk.dhfs.objects.repository.PersistentRemoteHostsService;
import com.usatiuk.utils.StatusRuntimeExceptionNoStacktrace;
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

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@ApplicationScoped
public class DhfsFileServiceImpl implements DhfsFileService {
    @Inject
    JObjectManager jObjectManager;

    @ConfigProperty(name = "dhfs.files.target_chunk_size")
    Integer targetChunkSize;

    @ConfigProperty(name = "dhfs.files.write_merge_threshold")
    float writeMergeThreshold;

    @ConfigProperty(name = "dhfs.files.write_merge_max_chunk_to_take")
    float writeMergeMaxChunkToTake;

    @ConfigProperty(name = "dhfs.files.write_merge_limit")
    float writeMergeLimit;

    @ConfigProperty(name = "dhfs.files.write_last_chunk_limit")
    float writeLastChunkLimit;

    @ConfigProperty(name = "dhfs.files.use_hash_for_chunks")
    boolean useHashForChunks;

    @Inject
    PersistentRemoteHostsService persistentRemoteHostsService;

    private ChunkData createChunk(ByteString bytes) {
        if (useHashForChunks) {
            return new ChunkData(bytes);
        } else {
            return new ChunkData(bytes, persistentRemoteHostsService.getUniqueId());
        }
    }

    void init(@Observes @Priority(500) StartupEvent event) {
        Log.info("Initializing file service");
        if (jObjectManager.get(new UUID(0, 0).toString()).isEmpty())
            jObjectManager.put(new Directory(new UUID(0, 0), 0755), Optional.empty());
        getRoot();
    }

    private JObject<? extends FsNode> traverse(JObject<? extends FsNode> from, Path path) {
        if (path.getNameCount() == 0) return from;

        var pathFirstPart = path.getName(0).toString();

        var notFound = new StatusRuntimeExceptionNoStacktrace(Status.NOT_FOUND.withDescription("Not found: " + from.getName() + "/" + path));

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
        Log.trace("Creating file " + fuuid);
        File f = new File(fuuid, mode, UUID.fromString(parent.getName()), false);

        if (!parent.runWriteLocked(JObject.ResolutionStrategy.REMOTE, (m, d, bump, invalidate) -> {
            if (!(d instanceof Directory dir))
                return false;

            if (dir.getKid(fname).isPresent())
                return false;

            bump.apply();

            boolean created = dir.putKid(fname, fuuid);
            if (!created) return false;

            jObjectManager.put(f, Optional.of(dir.getName()));

            dir.setMtime(System.currentTimeMillis());
            return true;
        }))
            return Optional.empty();

        return Optional.of(f.getName());
    }

    @Override
    public Optional<String> mkdir(String name, long mode) {
        var found = traverse(getRoot(), Path.of(name).getParent());

        String dname = Path.of(name).getFileName().toString();

        var duuid = UUID.randomUUID();
        Log.trace("Creating dir " + duuid);
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

        var kid = jObjectManager.get(kidId.get().toString());

        if (kid.isEmpty()) return false;

        return parent.runWriteLocked(JObject.ResolutionStrategy.REMOTE, (m, d, bump, i) -> {
            if (!(d instanceof Directory dir))
                return false;

            String kname = Path.of(name).getFileName().toString();

            if (dir.getKid(kname).isEmpty())
                return false;

            kid.get().runWriteLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (m2, d2, bump2, i2) -> {
                m2.removeRef(m.getName());
                return null;
            });

            boolean removed = dir.removeKid(kname);

            bump.apply();
            dir.setMtime(System.currentTimeMillis());
            return removed;
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
                theFile.getMeta().removeRef(dentFrom.getName());
                theFile.getMeta().addRef(dentTo.getName());
            } else if (theFile.getData() instanceof File f) {
                var newFile = new File(UUID.randomUUID(), f.getMode(), UUID.fromString(dentTo.getName()), f.isSymlink());
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

                theFile.getMeta().removeRef(dentFrom.getName());
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
            theFile.notifyWrite();
            theFile.rwUnlock();
        }

        if (cleanup != null) {
            jObjectManager.get(cleanup.toString()).ifPresent(c -> {
                c.runWriteLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (m, d, b, i) -> {
                    m.removeRef(dentTo.getName());
                    return null;
                });
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
        if (length < 0)
            throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("Length should be more than zero: " + length));
        if (offset < 0)
            throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("Offset should be more than zero: " + offset));

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

            if (toReadReally < 0) break;

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
        getChunkSize(uuid); // FIXME: This uncovers an ugly truth that ChunkData has truly the file as a parent, not ChunkInfo

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

    private void cleanupChunks(File f, Collection<String> uuids) {
        // FIXME:
        var inFile = useHashForChunks ? new HashSet<>(f.getChunks().values()) : Collections.emptySet();
        for (var cuuid : uuids) {
            try {
                if (inFile.contains(cuuid)) continue;
                var ci = jObjectManager.get(ChunkInfo.getNameFromHash(cuuid));
                if (ci.isPresent()) {
                    ci.get().runWriteLocked(JObject.ResolutionStrategy.NO_RESOLUTION, (m, d, b, v) -> {
                        m.removeRef(f.getName());
                        return null;
                    });
                }
            } catch (Exception e) {
                Log.error("Error when cleaning chunk " + cuuid, e);
            }
        }
    }

    @Override
    public Long write(String fileUuid, long offset, byte[] data) {
        if (offset < 0)
            throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("Offset should be more than zero: " + offset));

        var fileOpt = jObjectManager.get(fileUuid);
        if (fileOpt.isEmpty()) {
            Log.error("File not found when trying to read: " + fileUuid);
            return -1L;
        }
        var file = fileOpt.get();

        // FIXME:
        file.runWriteLocked(JObject.ResolutionStrategy.REMOTE, (meta, fDataU, bump, i) -> {
            if (!(fDataU instanceof File fData))
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT);

            if (size(fileUuid) < offset)
                truncate(fileUuid, offset);

            var chunksAll = fData.getChunks();
            var first = chunksAll.floorEntry(offset);
            var last = chunksAll.lowerEntry(offset + data.length);
            TreeSet<String> removedChunks = new TreeSet<>();
            try {
                long start = 0;

                NavigableMap<Long, String> beforeFirst = first != null ? chunksAll.headMap(first.getKey(), false) : Collections.emptyNavigableMap();
                NavigableMap<Long, String> afterLast = last != null ? chunksAll.tailMap(last.getKey(), false) : Collections.emptyNavigableMap();

                if (first != null && (getChunkSize(first.getValue()) + first.getKey() <= offset)) {
                    beforeFirst = chunksAll;
                    afterLast = Collections.emptyNavigableMap();
                    first = null;
                    last = null;
                    start = offset;
                } else if (!chunksAll.isEmpty()) {
                    var between = chunksAll.subMap(first.getKey(), true, last.getKey(), true);
                    removedChunks.addAll(between.values());
                    start = first.getKey();
                    between.clear();
                }

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

                if (targetChunkSize > 0) {
                    if (combinedSize < (targetChunkSize * writeMergeThreshold)) {
                        boolean leftDone = false;
                        boolean rightDone = false;
                        while (!leftDone && !rightDone) {
                            if (beforeFirst.isEmpty()) leftDone = true;
                            if (!beforeFirst.isEmpty() && !leftDone) {
                                var takeLeft = beforeFirst.lastEntry();

                                var cuuid = takeLeft.getValue();

                                if (getChunkSize(cuuid) >= (targetChunkSize * writeMergeMaxChunkToTake)) {
                                    leftDone = true;
                                    continue;
                                }

                                if ((combinedSize + getChunkSize(cuuid)) > (targetChunkSize * writeMergeLimit)) {
                                    leftDone = true;
                                    continue;
                                }

                                beforeFirst.pollLastEntry();
                                start = takeLeft.getKey();
                                pendingWrites = readChunk(cuuid).concat(pendingWrites);
                                combinedSize += getChunkSize(cuuid);
                                chunksAll.remove(takeLeft.getKey());
                                removedChunks.add(cuuid);
                            }
                            if (afterLast.isEmpty()) rightDone = true;
                            if (!afterLast.isEmpty() && !rightDone) {
                                var takeRight = afterLast.firstEntry();

                                var cuuid = takeRight.getValue();

                                if (getChunkSize(cuuid) >= (targetChunkSize * writeMergeMaxChunkToTake)) {
                                    rightDone = true;
                                    continue;
                                }

                                if ((combinedSize + getChunkSize(cuuid)) > (targetChunkSize * writeMergeLimit)) {
                                    rightDone = true;
                                    continue;
                                }

                                afterLast.pollFirstEntry();
                                pendingWrites = pendingWrites.concat(readChunk(cuuid));
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

                        if (targetChunkSize <= 0)
                            end = combinedSize;
                        else {
                            if ((combinedSize - cur) > (targetChunkSize * writeLastChunkLimit)) {
                                end = Math.min(cur + targetChunkSize, combinedSize);
                            } else {
                                end = combinedSize;
                            }
                        }

                        var thisChunk = pendingWrites.substring(cur, end);

                        ChunkData newChunkData = createChunk(thisChunk);
                        ChunkInfo newChunkInfo = new ChunkInfo(newChunkData.getHash(), newChunkData.getBytes().size());
                        //FIXME:
                        jObjectManager.put(newChunkData, Optional.of(newChunkInfo.getName()));
                        jObjectManager.put(newChunkInfo, Optional.of(meta.getName()));
                        chunksAll.put(start, newChunkInfo.getHash());

                        start += thisChunk.size();
                        cur = end;
                    }
                }

                bump.apply();
                fData.setMtime(System.currentTimeMillis());
            } finally {
                cleanupChunks(fData, removedChunks);
            }
            return null;
        });


        return (long) data.length;
    }

    @Override
    public Boolean truncate(String fileUuid, long length) {
        if (length < 0)
            throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("Length should be more than zero: " + length));

        var fileOpt = jObjectManager.get(fileUuid);
        if (fileOpt.isEmpty()) {
            Log.error("File not found when trying to read: " + fileUuid);
            return false;
        }
        var file = fileOpt.get();

        if (length == 0) {
            try {
                file.runWriteLocked(JObject.ResolutionStrategy.REMOTE, (m, fileData, bump, i) -> {
                    if (!(fileData instanceof File f))
                        throw new StatusRuntimeException(Status.INVALID_ARGUMENT);
                    bump.apply();
                    var oldChunks = new LinkedHashSet<>(f.getChunks().values());
                    f.getChunks().clear();
                    f.setMtime(System.currentTimeMillis());
                    cleanupChunks(f, oldChunks);
                    return null;
                });
            } catch (Exception e) {
                Log.error("Error writing file chunks: " + fileUuid, e);
                return false;
            }
            return true;
        }

        try {
            file.runWriteLocked(JObject.ResolutionStrategy.REMOTE, (m, fDataU, bump, i) -> {
                if (!(fDataU instanceof File fData))
                    throw new StatusRuntimeException(Status.INVALID_ARGUMENT);

                var curSize = size(fileUuid);
                if (curSize == length) return null;

                var chunksAll = fData.getChunks();

                var removedChunks = new LinkedHashSet<String>();

                try {

                    if (curSize < length) {
                        long combinedSize = (length - curSize);

                        long start = curSize;

                        // Hack
                        HashMap<Long, ByteString> zeroCache = new HashMap<>();

                        {
                            long cur = 0;
                            while (cur < combinedSize) {
                                long end;

                                if (targetChunkSize <= 0)
                                    end = combinedSize;
                                else {
                                    if ((combinedSize - cur) > (targetChunkSize * 1.5)) {
                                        end = cur + targetChunkSize;
                                    } else {
                                        end = combinedSize;
                                    }
                                }

                                if (!zeroCache.containsKey(end - cur))
                                    zeroCache.put(end - cur, UnsafeByteOperations.unsafeWrap(new byte[Math.toIntExact(end - cur)]));

                                ChunkData newChunkData = createChunk(zeroCache.get(end - cur));
                                ChunkInfo newChunkInfo = new ChunkInfo(newChunkData.getHash(), newChunkData.getBytes().size());
                                //FIXME:
                                jObjectManager.put(newChunkData, Optional.of(newChunkInfo.getName()));
                                jObjectManager.put(newChunkInfo, Optional.of(m.getName()));
                                chunksAll.put(start, newChunkInfo.getHash());

                                start += newChunkInfo.getSize();
                                cur = end;
                            }
                        }
                    } else {
                        var tail = chunksAll.lowerEntry(length);
                        var afterTail = chunksAll.tailMap(tail.getKey(), false);

                        removedChunks.addAll(afterTail.values());
                        afterTail.clear();

                        var tailBytes = readChunk(tail.getValue());
                        var newChunk = tailBytes.substring(0, (int) (length - tail.getKey()));

                        chunksAll.remove(tail.getKey());
                        removedChunks.add(tail.getValue());

                        ChunkData newChunkData = createChunk(newChunk);
                        ChunkInfo newChunkInfo = new ChunkInfo(newChunkData.getHash(), newChunkData.getBytes().size());
                        //FIXME:
                        jObjectManager.put(newChunkData, Optional.of(newChunkInfo.getName()));
                        jObjectManager.put(newChunkInfo, Optional.of(m.getName()));
                        chunksAll.put(tail.getKey(), newChunkInfo.getHash());
                    }

                    bump.apply();
                    fData.setMtime(System.currentTimeMillis());
                } finally {
                    cleanupChunks(fData, removedChunks);
                }

                return null;
            });
        } catch (Exception e) {
            Log.error("Error reading file: " + fileUuid, e);
            return false;
        }
        return true;
    }

    @Override
    public String readlink(String uuid) {
        return readlinkBS(uuid).toStringUtf8();
    }

    @Override
    public ByteString readlinkBS(String uuid) {
        var fileOpt = jObjectManager.get(uuid).orElseThrow(() -> new StatusRuntimeException(Status.NOT_FOUND.withDescription("File not found when trying to readlink: " + uuid)));

        return fileOpt.runReadLocked(JObject.ResolutionStrategy.REMOTE, (md, fileData) -> {
            if (!(fileData instanceof File)) {
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT);
            }

            if (!((File) fileData).isSymlink())
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("Not a symlink: " + uuid));

            return read(uuid, 0, Math.toIntExact(size(uuid))).get();
        });
    }

    @Override
    public String symlink(String oldpath, String newpath) {
        var parent = traverse(getRoot(), Path.of(newpath).getParent());

        String fname = Path.of(newpath).getFileName().toString();

        var fuuid = UUID.randomUUID();
        Log.trace("Creating file " + fuuid);
        File f = new File(fuuid, 0, UUID.fromString(parent.getName()), true);

        ChunkData newChunkData = createChunk(UnsafeByteOperations.unsafeWrap(oldpath.getBytes(StandardCharsets.UTF_8)));
        ChunkInfo newChunkInfo = new ChunkInfo(newChunkData.getHash(), newChunkData.getBytes().size());

        f.getChunks().put(0L, newChunkInfo.getHash());

        if (!parent.runWriteLocked(JObject.ResolutionStrategy.REMOTE, (m, d, bump, invalidate) -> {
            if (!(d instanceof Directory dir))
                return false;

            if (dir.getKid(fname).isPresent())
                return false;

            bump.apply();

            boolean created = dir.putKid(fname, fuuid);
            if (!created) return false;

            jObjectManager.put(newChunkData, Optional.of(newChunkInfo.getName()));
            jObjectManager.put(newChunkInfo, Optional.of(f.getName()));
            jObjectManager.put(f, Optional.of(dir.getName()));

            dir.setMtime(System.currentTimeMillis());
            return true;
        })) return null;

        return f.getName();
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
        var read = jObjectManager.get(uuid)
                .orElseThrow(() -> new StatusRuntimeException(Status.NOT_FOUND));

        try {
            return read.runReadLocked(JObject.ResolutionStrategy.REMOTE, (fsNodeData, fileData) -> {
                if (!(fileData instanceof File fd))
                    throw new StatusRuntimeException(Status.INVALID_ARGUMENT);

                var last = fd.getChunks().lastEntry();

                if (last == null)
                    return 0L;

                var lastSize = getChunkSize(last.getValue());
                return last.getKey() + lastSize;
            });
        } catch (Exception e) {
            Log.error("Error reading file: " + uuid, e);
            return -1L;
        }
    }

    private JObject<Directory> getRoot() {
        var read = jObjectManager.get(new UUID(0, 0).toString());
        if (read.isEmpty()) {
            Log.error("Root directory not found");
        }
        return (JObject<Directory>) read.get();
    }
}
