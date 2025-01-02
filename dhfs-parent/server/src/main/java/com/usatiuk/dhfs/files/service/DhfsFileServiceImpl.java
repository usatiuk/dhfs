package com.usatiuk.dhfs.files.service;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import com.usatiuk.dhfs.files.objects.ChunkData;
import com.usatiuk.dhfs.files.objects.File;
import com.usatiuk.dhfs.objects.TransactionManager;
import com.usatiuk.dhfs.objects.jkleppmanntree.JKleppmannTreeManager;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNode;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNodeMeta;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNodeMetaDirectory;
import com.usatiuk.dhfs.objects.jkleppmanntree.structs.JKleppmannTreeNodeMetaFile;
import com.usatiuk.dhfs.objects.transaction.LockingStrategy;
import com.usatiuk.dhfs.objects.transaction.Transaction;
import com.usatiuk.dhfs.utils.StatusRuntimeExceptionNoStacktrace;
import com.usatiuk.dhfs.objects.JData;
import com.usatiuk.dhfs.objects.JObjectKey;
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
    Transaction curTx;
    @Inject
    TransactionManager jObjectTxManager;

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
    JKleppmannTreeManager jKleppmannTreeManager;

    private JKleppmannTreeManager.JKleppmannTree getTree() {
        return jKleppmannTreeManager.getTree(new JObjectKey("fs"));
    }

    private ChunkData createChunk(ByteString bytes) {
        var newChunk = new ChunkData(JObjectKey.of(UUID.randomUUID().toString()), bytes);
        curTx.put(newChunk);
        return newChunk;
    }

    void init(@Observes @Priority(500) StartupEvent event) {
        Log.info("Initializing file service");
        getTree();
    }

    private JKleppmannTreeNode getDirEntry(String name) {
        var res = getTree().traverse(StreamSupport.stream(Path.of(name).spliterator(), false).map(p -> p.toString()).toList());
        if (res == null) throw new StatusRuntimeExceptionNoStacktrace(Status.NOT_FOUND);
        var ret = curTx.get(JKleppmannTreeNode.class, res).orElseThrow(() -> new StatusRuntimeException(Status.NOT_FOUND.withDescription("Tree node exists but not found as jObject: " + name)));
        return ret;
    }

    private Optional<JKleppmannTreeNode> getDirEntryOpt(String name) {
        var res = getTree().traverse(StreamSupport.stream(Path.of(name).spliterator(), false).map(p -> p.toString()).toList());
        if (res == null) return Optional.empty();
        var ret = curTx.get(JKleppmannTreeNode.class, res);
        return ret;
    }

    @Override
    public Optional<GetattrRes> getattr(JObjectKey uuid) {
        return jObjectTxManager.executeTx(() -> {
            var ref = curTx.get(JData.class, uuid).orElse(null);
            if (ref == null) return Optional.empty();
            GetattrRes ret;
            if (ref instanceof File f) {
                ret = new GetattrRes(f.mTime(), f.cTime(), f.mode(), f.symlink() ? GetattrType.SYMLINK : GetattrType.FILE);
            } else if (ref instanceof JKleppmannTreeNode) {
                ret = new GetattrRes(100, 100, 0700, GetattrType.DIRECTORY);
            } else {
                throw new StatusRuntimeException(Status.DATA_LOSS.withDescription("FsNode is not an FsNode: " + ref.key()));
            }
            return Optional.of(ret);
        });
    }

    @Override
    public Optional<JObjectKey> open(String name) {
        return jObjectTxManager.executeTx(() -> {
            try {
                var ret = getDirEntry(name);
                return switch (ret.meta()) {
                    case JKleppmannTreeNodeMetaFile f -> Optional.of(f.getFileIno());
                    case JKleppmannTreeNodeMetaDirectory f -> Optional.of(ret.key());
                    default -> Optional.empty();
                };
            } catch (StatusRuntimeException e) {
                if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
                    return Optional.empty();
                }
                throw e;
            }
        });
    }

    private void ensureDir(JKleppmannTreeNode entry) {
        if (!(entry.meta() instanceof JKleppmannTreeNodeMetaDirectory))
            throw new StatusRuntimeExceptionNoStacktrace(Status.INVALID_ARGUMENT.withDescription("Not a directory: " + entry.key()));
    }

    @Override
    public Optional<JObjectKey> create(String name, long mode) {
        return jObjectTxManager.executeTx(() -> {
            Path path = Path.of(name);
            var parent = getDirEntry(path.getParent().toString());

            ensureDir(parent);

            String fname = path.getFileName().toString();

            var fuuid = UUID.randomUUID();
            Log.debug("Creating file " + fuuid);
            File f = new File(JObjectKey.of(fuuid.toString()), new HashSet<>(), false, mode, System.currentTimeMillis(), System.currentTimeMillis(), new TreeMap<>(), false, 0);
            curTx.put(f);

            try {
                getTree().move(parent.key(), new JKleppmannTreeNodeMetaFile(fname, f.key()), getTree().getNewNodeId());
            } catch (Exception e) {
//                fobj.getMeta().removeRef(newNodeId);
                throw e;
            } finally {
//                fobj.rwUnlock();
            }
            return Optional.of(f.key());
        });
    }

    //FIXME: Slow..
    @Override
    public Pair<String, JObjectKey> inoToParent(JObjectKey ino) {
        return jObjectTxManager.executeTx(() -> {
            return getTree().findParent(w -> {
                if (w.meta() instanceof JKleppmannTreeNodeMetaFile f)
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

            getTree().move(parent.key(), new JKleppmannTreeNodeMetaDirectory(dname), getTree().getNewNodeId());
        });
    }

    @Override
    public void unlink(String name) {
        jObjectTxManager.executeTx(() -> {
            var node = getDirEntryOpt(name).orElse(null);
            if (node.meta() instanceof JKleppmannTreeNodeMetaDirectory f) {
                if (!allowRecursiveDelete && !node.children().isEmpty())
                    throw new DirectoryNotEmptyException();
            }
            getTree().trash(node.meta(), node.key());
        });
    }

    @Override
    public Boolean rename(String from, String to) {
        return jObjectTxManager.executeTx(() -> {
            var node = getDirEntry(from);
            JKleppmannTreeNodeMeta meta = node.meta();

            var toPath = Path.of(to);
            var toDentry = getDirEntry(toPath.getParent().toString());
            ensureDir(toDentry);

            getTree().move(toDentry.key(), meta.withName(toPath.getFileName().toString()), node.key());
            return true;
        });
    }

    @Override
    public Boolean chmod(JObjectKey uuid, long mode) {
        return jObjectTxManager.executeTx(() -> {
            var dent = curTx.get(JData.class, uuid).orElseThrow(() -> new StatusRuntimeExceptionNoStacktrace(Status.NOT_FOUND));

            if (dent instanceof JKleppmannTreeNode) {
                return true;
            } else if (dent instanceof File f) {
                curTx.put(f.toBuilder().mode(mode).mTime(System.currentTimeMillis()).build());
                return true;
            } else {
                throw new IllegalArgumentException(uuid + " is not a file");
            }
        });
    }

    @Override
    public Iterable<String> readDir(String name) {
        return jObjectTxManager.executeTx(() -> {
            var found = getDirEntry(name);

            if (!(found.meta() instanceof JKleppmannTreeNodeMetaDirectory md))
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT);

            return found.children().keySet();
        });
    }

    @Override
    public Optional<ByteString> read(JObjectKey fileUuid, long offset, int length) {
        return jObjectTxManager.executeTx(() -> {
            if (length < 0)
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("Length should be more than zero: " + length));
            if (offset < 0)
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("Offset should be more than zero: " + offset));

            var file = curTx.get(File.class, fileUuid).orElse(null);
            if (file == null) {
                Log.error("File not found when trying to read: " + fileUuid);
                return Optional.empty();
            }

            try {
                var chunksAll = file.chunks();
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
            } catch (Exception e) {
                Log.error("Error reading file: " + fileUuid, e);
                return Optional.empty();
            }
        });
    }

    private ByteString readChunk(JObjectKey uuid) {
        var chunkRead = curTx.get(ChunkData.class, uuid).orElse(null);

        if (chunkRead == null) {
            Log.error("Chunk requested not found: " + uuid);
            throw new StatusRuntimeException(Status.NOT_FOUND);
        }

        return chunkRead.data();
    }

    private int getChunkSize(JObjectKey uuid) {
        return readChunk(uuid).size();
    }

    private void cleanupChunks(File f, Collection<JObjectKey> uuids) {
        // FIXME:
//        var inFile = useHashForChunks ? new HashSet<>(f.getChunks().values()) : Collections.emptySet();
//        for (var cuuid : uuids) {
//            try {
//                if (inFile.contains(cuuid)) continue;
//                jObjectManager.get(cuuid)
//                        .ifPresent(jObject -> jObject.runWriteLocked(JObjectManager.ResolutionStrategy.NO_RESOLUTION,
//                                (m, d, b, v) -> {
//                                    m.removeRef(f.getName());
//                                    return null;
//                                }));
//            } catch (Exception e) {
//                Log.error("Error when cleaning chunk " + cuuid, e);
//            }
//        }
    }

    @Override
    public Long write(JObjectKey fileUuid, long offset, ByteString data) {
        return jObjectTxManager.executeTx(() -> {
            if (offset < 0)
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("Offset should be more than zero: " + offset));

            // FIXME:
            var file = curTx.get(File.class, fileUuid, LockingStrategy.WRITE).orElse(null);
            if (file == null) {
                Log.error("File not found when trying to write: " + fileUuid);
                return -1L;
            }

            if (writeLogging) {
                Log.info("Writing to file: " + file.key() + " size=" + size(fileUuid) + " "
                        + offset + " " + data.size());
            }

            if (size(fileUuid) < offset) {
                truncate(fileUuid, offset);
                file = curTx.get(File.class, fileUuid).orElse(null);
            }

            // FIXME: Some kind of immutable interface?
            var chunksAll = Collections.unmodifiableNavigableMap(file.chunks());
            var first = chunksAll.floorEntry(offset);
            var last = chunksAll.lowerEntry(offset + data.size());
            NavigableMap<Long, JObjectKey> removedChunks = new TreeMap<>();

            long start = 0;

            NavigableMap<Long, JObjectKey> beforeFirst = first != null ? chunksAll.headMap(first.getKey(), false) : Collections.emptyNavigableMap();
            NavigableMap<Long, JObjectKey> afterLast = last != null ? chunksAll.tailMap(last.getKey(), false) : Collections.emptyNavigableMap();

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

            NavigableMap<Long, JObjectKey> newChunks = new TreeMap<>();

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
                    newChunks.put(start, newChunkData.key());

                    start += thisChunk.size();
                    cur = end;
                }
            }

            NavigableMap<Long, JObjectKey> realNewChunks = new TreeMap<>();
            for (var chunk : chunksAll.entrySet()) {
                if (!removedChunks.containsKey(chunk.getKey())) {
                    realNewChunks.put(chunk.getKey(), chunk.getValue());
                }
            }

            realNewChunks.putAll(newChunks);

            file = file.toBuilder().chunks(Collections.unmodifiableNavigableMap(realNewChunks)).mTime(System.currentTimeMillis()).build();
            curTx.put(file);
            cleanupChunks(file, removedChunks.values());
            updateFileSize(file);

            return (long) data.size();
        });
    }

    @Override
    public Boolean truncate(JObjectKey fileUuid, long length) {
        return jObjectTxManager.executeTx(() -> {
            if (length < 0)
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("Length should be more than zero: " + length));

            var file = curTx.get(File.class, fileUuid).orElse(null);
            if (file == null) {
                Log.error("File not found when trying to write: " + fileUuid);
                return false;
            }

            if (length == 0) {
                var oldChunks = Collections.unmodifiableNavigableMap(new TreeMap<>(file.chunks()));

                file = file.toBuilder().chunks(new TreeMap<>()).mTime(System.currentTimeMillis()).build();
                curTx.put(file);
                cleanupChunks(file, oldChunks.values());
                updateFileSize(file);
                return true;
            }

            var curSize = size(fileUuid);
            if (curSize == length) return true;

            var chunksAll = Collections.unmodifiableNavigableMap(file.chunks());
            NavigableMap<Long, JObjectKey> removedChunks = new TreeMap<>();
            NavigableMap<Long, JObjectKey> newChunks = new TreeMap<>();

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
                        newChunks.put(start, newChunkData.key());

                        start += newChunkData.data().size();
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
                newChunks.put(tail.getKey(), newChunkData.key());
            }

            NavigableMap<Long, JObjectKey> realNewChunks = new TreeMap<>();
            for (var chunk : chunksAll.entrySet()) {
                if (!removedChunks.containsKey(chunk.getKey())) {
                    realNewChunks.put(chunk.getKey(), chunk.getValue());
                }
            }

            realNewChunks.putAll(newChunks);

            file = file.toBuilder().chunks(Collections.unmodifiableNavigableMap(realNewChunks)).mTime(System.currentTimeMillis()).build();
            curTx.put(file);
            cleanupChunks(file, removedChunks.values());
            updateFileSize(file);
            return true;
        });
    }

    @Override
    public String readlink(JObjectKey uuid) {
        return jObjectTxManager.executeTx(() -> {
            return readlinkBS(uuid).toStringUtf8();
        });
    }

    @Override
    public ByteString readlinkBS(JObjectKey uuid) {
        return jObjectTxManager.executeTx(() -> {
            var fileOpt = curTx.get(File.class, uuid).orElseThrow(() -> new StatusRuntimeException(Status.NOT_FOUND.withDescription("File not found when trying to readlink: " + uuid)));
            return read(uuid, 0, Math.toIntExact(size(uuid))).get();
        });
    }

    @Override
    public JObjectKey symlink(String oldpath, String newpath) {
        return jObjectTxManager.executeTx(() -> {
            Path path = Path.of(newpath);
            var parent = getDirEntry(path.getParent().toString());

            ensureDir(parent);

            String fname = path.getFileName().toString();

            var fuuid = UUID.randomUUID();
            Log.debug("Creating file " + fuuid);

            File f = new File(JObjectKey.of(fuuid.toString()), new HashSet<>(), false, 0, System.currentTimeMillis(), System.currentTimeMillis(), new TreeMap<>(), true, 0);
            ChunkData newChunkData = createChunk(UnsafeByteOperations.unsafeWrap(oldpath.getBytes(StandardCharsets.UTF_8)));

            f.chunks().put(0L, newChunkData.key());
            updateFileSize(f);

            getTree().move(parent.key(), new JKleppmannTreeNodeMetaFile(fname, f.key()), getTree().getNewNodeId());
            return f.key();
        });
    }

    @Override
    public Boolean setTimes(JObjectKey fileUuid, long atimeMs, long mtimeMs) {
        return jObjectTxManager.executeTx(() -> {
            var file = curTx.get(File.class, fileUuid).orElseThrow(
                    () -> new StatusRuntimeException(Status.NOT_FOUND.withDescription(
                            "File not found for setTimes: " + fileUuid))
            );

            curTx.put(file.toBuilder().cTime(atimeMs).mTime(mtimeMs).build());
            return true;
        });
    }

    @Override
    public void updateFileSize(File file) {
        jObjectTxManager.executeTx(() -> {
            long realSize = 0;

            var last = file.chunks().lastEntry();
            if (last != null) {
                var lastSize = getChunkSize(last.getValue());
                realSize = last.getKey() + lastSize;
            }

            if (realSize != file.size()) {
                curTx.put(file.toBuilder().size(realSize).build());
            }
        });
    }

    @Override
    public Long size(JObjectKey uuid) {
        return jObjectTxManager.executeTx(() -> {
            var read = curTx.get(File.class, uuid)
                    .orElseThrow(() -> new StatusRuntimeException(Status.NOT_FOUND));

            return read.size();
        });
    }
}
