package com.usatiuk.dhfs.files.service;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import com.usatiuk.dhfs.files.objects.ChunkData;
import com.usatiuk.dhfs.files.objects.File;
import com.usatiuk.dhfs.files.objects.FsNode;
import com.usatiuk.dhfs.objects.jkleppmanntree.JKleppmannTreeManager;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNode;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNodeMeta;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNodeMetaDirectory;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNodeMetaFile;
import com.usatiuk.dhfs.objects.jrepository.JMutator;
import com.usatiuk.dhfs.objects.jrepository.JObject;
import com.usatiuk.dhfs.objects.jrepository.JObjectManager;
import com.usatiuk.dhfs.objects.jrepository.JObjectTxManager;
import com.usatiuk.dhfs.objects.repository.PersistentPeerDataService;
import com.usatiuk.dhfs.utils.StatusRuntimeExceptionNoStacktrace;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.logging.Log;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.StreamSupport;

@ApplicationScoped
public class DhfsFileServiceImpl implements DhfsFileService {
    @Inject
    JObjectManager jObjectManager;
    @Inject
    JObjectTxManager jObjectTxManager;

    @ConfigProperty(name = "dhfs.files.target_chunk_size")
    int targetChunkSize;

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

    @ConfigProperty(name = "dhfs.files.allow_recursive_delete")
    boolean allowRecursiveDelete;

    @ConfigProperty(name = "dhfs.objects.ref_verification")
    boolean refVerification;

    @ConfigProperty(name = "dhfs.objects.write_log")
    boolean writeLogging;

    @Inject
    PersistentPeerDataService persistentPeerDataService;
    @Inject
    JKleppmannTreeManager jKleppmannTreeManager;

    private JKleppmannTreeManager.JKleppmannTree _tree;

    private ChunkData createChunk(ByteString bytes) {
        if (useHashForChunks) {
            return new ChunkData(bytes);
        } else {
            return new ChunkData(bytes, persistentPeerDataService.getUniqueId());
        }
    }

    void init(@Observes @Priority(500) StartupEvent event) {
        Log.info("Initializing file service");
        _tree = jKleppmannTreeManager.getTree("fs");
    }

    private JObject<JKleppmannTreeNode> getDirEntry(String name) {
        var res = _tree.traverse(StreamSupport.stream(Path.of(name).spliterator(), false).map(p -> p.toString()).toList());
        if (res == null) throw new StatusRuntimeExceptionNoStacktrace(Status.NOT_FOUND);
        var ret = jObjectManager.get(res).orElseThrow(() -> new StatusRuntimeException(Status.NOT_FOUND.withDescription("Tree node exists but not found as jObject: " + name)));
        if (!ret.getMeta().getKnownClass().equals(JKleppmannTreeNode.class))
            throw new StatusRuntimeException(Status.NOT_FOUND.withDescription("Tree node exists but not jObject: " + name));
        return (JObject<JKleppmannTreeNode>) ret;
    }

    private Optional<JObject<JKleppmannTreeNode>> getDirEntryOpt(String name) {
        var res = _tree.traverse(StreamSupport.stream(Path.of(name).spliterator(), false).map(p -> p.toString()).toList());
        if (res == null) return Optional.empty();
        var ret = jObjectManager.get(res).orElseThrow(() -> new StatusRuntimeException(Status.NOT_FOUND.withDescription("Tree node exists but not found as jObject: " + name)));
        if (!ret.getMeta().getKnownClass().equals(JKleppmannTreeNode.class))
            throw new StatusRuntimeException(Status.NOT_FOUND.withDescription("Tree node exists but not jObject: " + name));
        return Optional.of((JObject<JKleppmannTreeNode>) ret);
    }

    @Override
    public Optional<GetattrRes> getattr(String uuid) {
        return jObjectTxManager.executeTx(() -> {
            var ref = jObjectManager.get(uuid);
            if (ref.isEmpty()) return Optional.empty();
            return ref.get().runReadLocked(JObjectManager.ResolutionStrategy.REMOTE, (m, d) -> {
                GetattrRes ret;
                if (d instanceof File f) {
                    ret = new GetattrRes(f.getMtime(), f.getCtime(), f.getMode(), f.isSymlink() ? GetattrType.SYMLINK : GetattrType.FILE);
                } else if (d instanceof JKleppmannTreeNode) {
                    ret = new GetattrRes(100, 100, 0700, GetattrType.DIRECTORY);
                } else {
                    throw new StatusRuntimeException(Status.DATA_LOSS.withDescription("FsNode is not an FsNode: " + m.getName()));
                }
                return Optional.of(ret);
            });
        });
    }

    @Override
    public Optional<String> open(String name) {
        return jObjectTxManager.executeTx(() -> {
            try {
                var ret = getDirEntry(name);
                return Optional.of(ret.runReadLocked(JObjectManager.ResolutionStrategy.LOCAL_ONLY, (m, d) -> {
                    if (d.getNode().getMeta() instanceof JKleppmannTreeNodeMetaFile f) return f.getFileIno();
                    else if (d.getNode().getMeta() instanceof JKleppmannTreeNodeMetaDirectory f) return m.getName();
                    throw new StatusRuntimeException(Status.DATA_LOSS.withDescription("FsNode is not an FsNode: " + m.getName()));
                }));
            } catch (StatusRuntimeException e) {
                if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
                    return Optional.empty();
                }
                throw e;
            }
        });
    }

    private void ensureDir(JObject<JKleppmannTreeNode> entry) {
        entry.runReadLocked(JObjectManager.ResolutionStrategy.REMOTE, (m, d) -> {
            if (d.getNode().getMeta() instanceof JKleppmannTreeNodeMetaFile f)
                throw new StatusRuntimeExceptionNoStacktrace(Status.INVALID_ARGUMENT.withDescription(m.getName() + " is a file, not directory"));
            else if (d.getNode().getMeta() instanceof JKleppmannTreeNodeMetaDirectory f) return null;
            throw new StatusRuntimeException(Status.DATA_LOSS.withDescription("FsNode is not an FsNode: " + m.getName()));
        });
    }

    @Override
    public Optional<String> create(String name, long mode) {
        return jObjectTxManager.executeTx(() -> {
            Path path = Path.of(name);
            var parent = getDirEntry(path.getParent().toString());

            ensureDir(parent);

            String fname = path.getFileName().toString();

            var fuuid = UUID.randomUUID();
            Log.debug("Creating file " + fuuid);
            File f = new File(fuuid, mode, false);

            var newNodeId = _tree.getNewNodeId();
            var fobj = jObjectManager.putLocked(f, Optional.of(newNodeId));
            try {
                _tree.move(parent.getMeta().getName(), new JKleppmannTreeNodeMetaFile(fname, f.getName()), newNodeId);
            } catch (Exception e) {
                fobj.getMeta().removeRef(newNodeId);
                throw e;
            } finally {
                fobj.rwUnlock();
            }
            return Optional.of(f.getName());
        });
    }

    //FIXME: Slow..
    @Override
    public Pair<String, String> inoToParent(String ino) {
        return jObjectTxManager.executeTx(() -> {
            return _tree.findParent(w -> {
                if (w.getNode().getMeta() instanceof JKleppmannTreeNodeMetaFile f)
                    if (f.getFileIno().equals(ino))
                        return true;
                return false;
            });
        });
    }

    @Override
    public void mkdir(String name, long mode) {
        jObjectTxManager.executeTx(() -> {
            Path path = Path.of(name);
            var parent = getDirEntry(path.getParent().toString());
            ensureDir(parent);

            String dname = path.getFileName().toString();

            Log.debug("Creating directory " + name);

            _tree.move(parent.getMeta().getName(), new JKleppmannTreeNodeMetaDirectory(dname), _tree.getNewNodeId());
        });
    }

    @Override
    public void unlink(String name) {
        jObjectTxManager.executeTx(() -> {
            var node = getDirEntryOpt(name).orElse(null);
            JKleppmannTreeNodeMeta meta = node.runReadLocked(JObjectManager.ResolutionStrategy.REMOTE, (m, d) -> {
                if (d.getNode().getMeta() instanceof JKleppmannTreeNodeMetaDirectory f)
                    if (!d.getNode().getChildren().isEmpty()) throw new DirectoryNotEmptyException();
                return d.getNode().getMeta();
            });

            _tree.trash(meta, node.getMeta().getName());
        });
    }

    @Override
    public Boolean rename(String from, String to) {
        return jObjectTxManager.executeTx(() -> {
            var node = getDirEntry(from);
            JKleppmannTreeNodeMeta meta = node.runReadLocked(JObjectManager.ResolutionStrategy.REMOTE, (m, d) -> d.getNode().getMeta());

            var toPath = Path.of(to);
            var toDentry = getDirEntry(toPath.getParent().toString());
            ensureDir(toDentry);

            _tree.move(toDentry.getMeta().getName(), meta.withName(toPath.getFileName().toString()), node.getMeta().getName());

            return true;
        });
    }

    @Override
    public Boolean chmod(String uuid, long mode) {
        return jObjectTxManager.executeTx(() -> {
            var dent = jObjectManager.get(uuid).orElseThrow(() -> new StatusRuntimeExceptionNoStacktrace(Status.NOT_FOUND));

            dent.runWriteLocked(JObjectManager.ResolutionStrategy.REMOTE, (m, d, bump, i) -> {
                if (d instanceof JKleppmannTreeNode) {
                    return null;//FIXME:?
                } else if (d instanceof File f) {
                    bump.apply();
                    f.setMtime(System.currentTimeMillis());
                    f.setMode(mode);
                } else {
                    throw new IllegalArgumentException(uuid + " is not a file");
                }
                return null;
            });

            return true;
        });
    }

    @Override
    public Iterable<String> readDir(String name) {
        return jObjectTxManager.executeTx(() -> {
            var found = getDirEntry(name);

            return found.runReadLocked(JObjectManager.ResolutionStrategy.REMOTE, (m, d) -> {
                if (!(d instanceof JKleppmannTreeNode) || !(d.getNode().getMeta() instanceof JKleppmannTreeNodeMetaDirectory)) {
                    throw new StatusRuntimeException(Status.INVALID_ARGUMENT);
                }
                return new ArrayList<>(d.getNode().getChildren().keySet());
            });
        });
    }

    @Override
    public Optional<ByteString> read(String fileUuid, long offset, int length) {
        return jObjectTxManager.executeTx(() -> {
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

            try {
                return file.runReadLocked(JObjectManager.ResolutionStrategy.REMOTE, (md, fileData) -> {
                    if (!(fileData instanceof File)) {
                        throw new StatusRuntimeException(Status.INVALID_ARGUMENT);
                    }
                    var chunksAll = ((File) fileData).getChunks();
                    if (chunksAll.isEmpty()) {
                        return Optional.of(ByteString.empty());
                    }
                    var chunksList = chunksAll.tailMap(chunksAll.floorKey(offset)).entrySet();

                    if (chunksList.isEmpty()) {
                        return Optional.of(ByteString.empty());
                    }

                    var chunks = chunksList.iterator();
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
                });
            } catch (Exception e) {
                Log.error("Error reading file: " + fileUuid, e);
                return Optional.empty();
            }
        });
    }

    private ByteString readChunk(String uuid) {
        var chunkRead = jObjectManager.get(uuid).orElse(null);

        if (chunkRead == null) {
            Log.error("Chunk requested not found: " + uuid);
            throw new StatusRuntimeException(Status.NOT_FOUND);
        }

        return chunkRead.runReadLocked(JObjectManager.ResolutionStrategy.REMOTE, (m, d) -> {
            if (!(d instanceof ChunkData cd))
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT);
            return cd.getBytes();
        });
    }

    private int getChunkSize(String uuid) {
        return readChunk(uuid).size();
    }

    private void cleanupChunks(File f, Collection<String> uuids) {
        // FIXME:
        var inFile = useHashForChunks ? new HashSet<>(f.getChunks().values()) : Collections.emptySet();
        for (var cuuid : uuids) {
            try {
                if (inFile.contains(cuuid)) continue;
                jObjectManager.get(cuuid)
                        .ifPresent(jObject -> jObject.runWriteLocked(JObjectManager.ResolutionStrategy.NO_RESOLUTION,
                                (m, d, b, v) -> {
                                    m.removeRef(f.getName());
                                    return null;
                                }));
            } catch (Exception e) {
                Log.error("Error when cleaning chunk " + cuuid, e);
            }
        }
    }

    @Override
    public Long write(String fileUuid, long offset, ByteString data) {
        return jObjectTxManager.executeTx(() -> {
            if (offset < 0)
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("Offset should be more than zero: " + offset));

            // FIXME:
            var file = (JObject<File>) jObjectManager.get(fileUuid).orElse(null);
            if (file == null) {
                Log.error("File not found when trying to read: " + fileUuid);
                return -1L;
            }

            file.rwLockNoCopy();
            try {
                file.tryResolve(JObjectManager.ResolutionStrategy.REMOTE);
                // FIXME:
                if (!(file.getData() instanceof File))
                    throw new StatusRuntimeException(Status.INVALID_ARGUMENT);

                if (writeLogging) {
                    Log.info("Writing to file: " + file.getMeta().getName() + " size=" + size(fileUuid) + " "
                            + offset + " " + data.size());
                }

                if (size(fileUuid) < offset)
                    truncate(fileUuid, offset);

                // FIXME: Some kind of immutable interface?
                var chunksAll = Collections.unmodifiableNavigableMap(file.getData().getChunks());
                var first = chunksAll.floorEntry(offset);
                var last = chunksAll.lowerEntry(offset + data.size());
                NavigableMap<Long, String> removedChunks = new TreeMap<>();

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
                    removedChunks.putAll(between);
                    start = first.getKey();
                }

                ByteString pendingWrites = ByteString.empty();

                if (first != null && first.getKey() < offset) {
                    var chunkBytes = readChunk(first.getValue());
                    pendingWrites = pendingWrites.concat(chunkBytes.substring(0, (int) (offset - first.getKey())));
                }
                pendingWrites = pendingWrites.concat(data);

                if (last != null) {
                    var lchunkBytes = readChunk(last.getValue());
                    if (last.getKey() + lchunkBytes.size() > offset + data.size()) {
                        var startInFile = offset + data.size();
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
                            if (!beforeFirst.isEmpty() || !leftDone) {
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

                                // FIXME: (and test this)
                                beforeFirst = beforeFirst.headMap(takeLeft.getKey(), false);
                                start = takeLeft.getKey();
                                pendingWrites = readChunk(cuuid).concat(pendingWrites);
                                combinedSize += getChunkSize(cuuid);
                                removedChunks.put(takeLeft.getKey(), takeLeft.getValue());
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

                                // FIXME: (and test this)
                                afterLast = afterLast.tailMap(takeRight.getKey(), false);
                                pendingWrites = pendingWrites.concat(readChunk(cuuid));
                                combinedSize += getChunkSize(cuuid);
                                removedChunks.put(takeRight.getKey(), takeRight.getValue());
                            }
                        }
                    }
                }

                NavigableMap<Long, String> newChunks = new TreeMap<>();

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
                        //FIXME:
                        jObjectManager.put(newChunkData, Optional.of(file.getMeta().getName()));
                        newChunks.put(start, newChunkData.getName());

                        start += thisChunk.size();
                        cur = end;
                    }
                }

                file.mutate(new FileChunkMutator(file.getData().getMtime(), System.currentTimeMillis(), removedChunks, newChunks));

                cleanupChunks(file.getData(), removedChunks.values());
                updateFileSize((JObject<File>) file);
            } finally {
                file.rwUnlock();
            }

            return (long) data.size();
        });
    }

    @Override
    public Boolean truncate(String fileUuid, long length) {
        return jObjectTxManager.executeTx(() -> {
            if (length < 0)
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("Length should be more than zero: " + length));

            var file = (JObject<File>) jObjectManager.get(fileUuid).orElse(null);
            if (file == null) {
                Log.error("File not found when trying to read: " + fileUuid);
                return false;
            }

            if (length == 0) {
                file.rwLockNoCopy();
                try {
                    file.tryResolve(JObjectManager.ResolutionStrategy.REMOTE);

                    var oldChunks = Collections.unmodifiableNavigableMap(new TreeMap<>(file.getData().getChunks()));

                    file.mutate(new JMutator<>() {
                        long oldMtime;

                        @Override
                        public boolean mutate(File object) {
                            oldMtime = object.getMtime();
                            object.getChunks().clear();
                            return true;
                        }

                        @Override
                        public void revert(File object) {
                            object.setMtime(oldMtime);
                            object.getChunks().putAll(oldChunks);
                        }
                    });
                    cleanupChunks(file.getData(), oldChunks.values());
                    updateFileSize((JObject<File>) file);
                } catch (Exception e) {
                    Log.error("Error writing file chunks: " + fileUuid, e);
                    return false;
                } finally {
                    file.rwUnlock();
                }
                return true;
            }

            file.rwLockNoCopy();
            try {
                file.tryResolve(JObjectManager.ResolutionStrategy.REMOTE);

                var curSize = size(fileUuid);
                if (curSize == length) return true;

                var chunksAll = Collections.unmodifiableNavigableMap(file.getData().getChunks());
                NavigableMap<Long, String> removedChunks = new TreeMap<>();
                NavigableMap<Long, String> newChunks = new TreeMap<>();

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
                            //FIXME:
                            jObjectManager.put(newChunkData, Optional.of(file.getMeta().getName()));
                            newChunks.put(start, newChunkData.getName());

                            start += newChunkData.getSize();
                            cur = end;
                        }
                    }
                } else {
                    var tail = chunksAll.lowerEntry(length);
                    var afterTail = chunksAll.tailMap(tail.getKey(), false);

                    removedChunks.put(tail.getKey(), tail.getValue());
                    removedChunks.putAll(afterTail);

                    var tailBytes = readChunk(tail.getValue());
                    var newChunk = tailBytes.substring(0, (int) (length - tail.getKey()));

                    ChunkData newChunkData = createChunk(newChunk);
                    //FIXME:
                    jObjectManager.put(newChunkData, Optional.of(file.getMeta().getName()));
                    newChunks.put(tail.getKey(), newChunkData.getName());
                }

                file.mutate(new FileChunkMutator(file.getData().getMtime(), System.currentTimeMillis(), removedChunks, newChunks));

                cleanupChunks(file.getData(), removedChunks.values());
                updateFileSize((JObject<File>) file);
                return true;
            } catch (Exception e) {
                Log.error("Error reading file: " + fileUuid, e);
                return false;
            } finally {
                file.rwUnlock();
            }
        });
    }

    @Override
    public String readlink(String uuid) {
        return jObjectTxManager.executeTx(() -> {
            return readlinkBS(uuid).toStringUtf8();
        });
    }

    @Override
    public ByteString readlinkBS(String uuid) {
        return jObjectTxManager.executeTx(() -> {
            var fileOpt = jObjectManager.get(uuid).orElseThrow(() -> new StatusRuntimeException(Status.NOT_FOUND.withDescription("File not found when trying to readlink: " + uuid)));

            return fileOpt.runReadLocked(JObjectManager.ResolutionStrategy.REMOTE, (md, fileData) -> {
                if (!(fileData instanceof File)) {
                    throw new StatusRuntimeException(Status.INVALID_ARGUMENT);
                }

                if (!((File) fileData).isSymlink())
                    throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("Not a symlink: " + uuid));

                return read(uuid, 0, Math.toIntExact(size(uuid))).get();
            });
        });
    }

    @Override
    public String symlink(String oldpath, String newpath) {
        return jObjectTxManager.executeTx(() -> {
            Path path = Path.of(newpath);
            var parent = getDirEntry(path.getParent().toString());

            ensureDir(parent);

            String fname = path.getFileName().toString();

            var fuuid = UUID.randomUUID();
            Log.debug("Creating file " + fuuid);

            File f = new File(fuuid, 0, true);
            var newNodeId = _tree.getNewNodeId();
            ChunkData newChunkData = createChunk(UnsafeByteOperations.unsafeWrap(oldpath.getBytes(StandardCharsets.UTF_8)));

            f.getChunks().put(0L, newChunkData.getName());

            jObjectManager.put(newChunkData, Optional.of(f.getName()));
            var newFile = jObjectManager.putLocked(f, Optional.of(newNodeId));
            try {
                updateFileSize(newFile);
            } finally {
                newFile.rwUnlock();
            }

            _tree.move(parent.getMeta().getName(), new JKleppmannTreeNodeMetaFile(fname, f.getName()), newNodeId);
            return f.getName();
        });
    }

    @Override
    public Boolean setTimes(String fileUuid, long atimeMs, long mtimeMs) {
        return jObjectTxManager.executeTx(() -> {
            var file = jObjectManager.get(fileUuid).orElseThrow(
                    () -> new StatusRuntimeException(Status.NOT_FOUND.withDescription(
                            "File not found for setTimes: " + fileUuid))
            );

            file.runWriteLocked(JObjectManager.ResolutionStrategy.REMOTE, (m, fileData, bump, i) -> {
                if (fileData instanceof JKleppmannTreeNode) return null; // FIXME:
                if (!(fileData instanceof FsNode fd))
                    throw new StatusRuntimeException(Status.INVALID_ARGUMENT);

                bump.apply();
                fd.setMtime(mtimeMs);
                return null;
            });

            return true;
        });
    }

    @Override
    public void updateFileSize(JObject<File> file) {
        jObjectTxManager.executeTx(() -> {
            file.rwLockNoCopy();
            try {
                file.tryResolve(JObjectManager.ResolutionStrategy.REMOTE);
                if (!(file.getData() instanceof File fd))
                    throw new StatusRuntimeException(Status.INVALID_ARGUMENT);

                long realSize = 0;

                var last = fd.getChunks().lastEntry();
                if (last != null) {
                    var lastSize = getChunkSize(last.getValue());
                    realSize = last.getKey() + lastSize;
                }

                if (realSize != fd.getSize()) {
                    long finalRealSize = realSize;
                    file.mutate(new JMutator<File>() {
                        long oldSize;

                        @Override
                        public boolean mutate(File object) {
                            oldSize = object.getSize();
                            object.setSize(finalRealSize);
                            return true;
                        }

                        @Override
                        public void revert(File object) {
                            object.setSize(oldSize);
                        }
                    });
                }
            } catch (Exception e) {
                Log.error("Error updating file size: " + file.getMeta().getName(), e);
            } finally {
                file.rwUnlock();
            }
        });
    }

    @Override
    public Long size(String uuid) {
        return jObjectTxManager.executeTx(() -> {
            var read = jObjectManager.get(uuid)
                    .orElseThrow(() -> new StatusRuntimeException(Status.NOT_FOUND));

            try {
                return read.runReadLocked(JObjectManager.ResolutionStrategy.REMOTE, (fsNodeData, fileData) -> {
                    if (!(fileData instanceof File fd))
                        throw new StatusRuntimeException(Status.INVALID_ARGUMENT);

                    return fd.getSize();
                });
            } catch (Exception e) {
                Log.error("Error reading file: " + uuid, e);
                return -1L;
            }
        });
    }
}
