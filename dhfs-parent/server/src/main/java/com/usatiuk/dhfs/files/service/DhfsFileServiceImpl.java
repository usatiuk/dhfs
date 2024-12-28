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
import com.usatiuk.dhfs.objects.transaction.Transaction;
import com.usatiuk.dhfs.utils.StatusRuntimeExceptionNoStacktrace;
import com.usatiuk.objects.alloc.runtime.ObjectAllocator;
import com.usatiuk.objects.common.runtime.JData;
import com.usatiuk.objects.common.runtime.JObjectKey;
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
    @Inject
    ObjectAllocator objectAllocator;

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
        var newChunk = objectAllocator.create(ChunkData.class, new JObjectKey(UUID.randomUUID().toString()));
        newChunk.setData(bytes);
        curTx.putObject(newChunk);
        return newChunk;
    }

    void init(@Observes @Priority(500) StartupEvent event) {
        Log.info("Initializing file service");
        getTree();
    }

    private JKleppmannTreeNode getDirEntry(String name) {
        var res = getTree().traverse(StreamSupport.stream(Path.of(name).spliterator(), false).map(p -> p.toString()).toList());
        if (res == null) throw new StatusRuntimeExceptionNoStacktrace(Status.NOT_FOUND);
        var ret = curTx.getObject(JKleppmannTreeNode.class, res).orElseThrow(() -> new StatusRuntimeException(Status.NOT_FOUND.withDescription("Tree node exists but not found as jObject: " + name)));
        return ret;
    }

    private Optional<JKleppmannTreeNode> getDirEntryOpt(String name) {
        var res = getTree().traverse(StreamSupport.stream(Path.of(name).spliterator(), false).map(p -> p.toString()).toList());
        if (res == null) return Optional.empty();
        var ret = curTx.getObject(JKleppmannTreeNode.class, res);
        return ret;
    }

    @Override
    public Optional<GetattrRes> getattr(JObjectKey uuid) {
        return jObjectTxManager.executeTx(() -> {
            var ref = curTx.getObject(JData.class, uuid).orElse(null);
            if (ref == null) return Optional.empty();
            GetattrRes ret;
            if (ref instanceof File f) {
                ret = new GetattrRes(f.getMtime(), f.getCtime(), f.getMode(), f.getSymlink() ? GetattrType.SYMLINK : GetattrType.FILE);
            } else if (ref instanceof JKleppmannTreeNode) {
                ret = new GetattrRes(100, 100, 0700, GetattrType.DIRECTORY);
            } else {
                throw new StatusRuntimeException(Status.DATA_LOSS.withDescription("FsNode is not an FsNode: " + ref.getKey()));
            }
            return Optional.of(ret);
        });
    }

    @Override
    public Optional<JObjectKey> open(String name) {
        return jObjectTxManager.executeTx(() -> {
            try {
                var ret = getDirEntry(name);
                return switch (ret.getNode().getMeta()) {
                    case JKleppmannTreeNodeMetaFile f -> Optional.of(f.getFileIno());
                    case JKleppmannTreeNodeMetaDirectory f -> Optional.of(ret.getKey());
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
        if (!(entry.getNode().getMeta() instanceof JKleppmannTreeNodeMetaDirectory))
            throw new StatusRuntimeExceptionNoStacktrace(Status.INVALID_ARGUMENT.withDescription("Not a directory: " + entry.getKey()));
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
            File f = objectAllocator.create(File.class, new JObjectKey(fuuid.toString()));
            f.setMode(mode);
            f.setMtime(System.currentTimeMillis());
            f.setCtime(f.getMtime());
            f.setSymlink(false);
            f.setChunks(new TreeMap<>());
            curTx.putObject(f);

            try {
                getTree().move(parent.getKey(), new JKleppmannTreeNodeMetaFile(fname, f.getKey()), getTree().getNewNodeId());
            } catch (Exception e) {
//                fobj.getMeta().removeRef(newNodeId);
                throw e;
            } finally {
//                fobj.rwUnlock();
            }
            return Optional.of(f.getKey());
        });
    }

    //FIXME: Slow..
    @Override
    public Pair<String, JObjectKey> inoToParent(JObjectKey ino) {
        return jObjectTxManager.executeTx(() -> {
            return getTree().findParent(w -> {
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

            getTree().move(parent.getKey(), new JKleppmannTreeNodeMetaDirectory(dname), getTree().getNewNodeId());
        });
    }

    @Override
    public void unlink(String name) {
        jObjectTxManager.executeTx(() -> {
            var node = getDirEntryOpt(name).orElse(null);
            if (node.getNode().getMeta() instanceof JKleppmannTreeNodeMetaDirectory f) {
                if (!allowRecursiveDelete && !node.getNode().getChildren().isEmpty())
                    throw new DirectoryNotEmptyException();
            }
            getTree().trash(node.getNode().getMeta(), node.getKey());
        });
    }

    @Override
    public Boolean rename(String from, String to) {
        return jObjectTxManager.executeTx(() -> {
            var node = getDirEntry(from);
            JKleppmannTreeNodeMeta meta = node.getNode().getMeta();

            var toPath = Path.of(to);
            var toDentry = getDirEntry(toPath.getParent().toString());
            ensureDir(toDentry);

            getTree().move(toDentry.getKey(), meta.withName(toPath.getFileName().toString()), node.getKey());
            return true;
        });
    }

    @Override
    public Boolean chmod(JObjectKey uuid, long mode) {
        return jObjectTxManager.executeTx(() -> {
            var dent = curTx.getObject(JData.class, uuid).orElseThrow(() -> new StatusRuntimeExceptionNoStacktrace(Status.NOT_FOUND));

            if (dent instanceof JKleppmannTreeNode) {
                return true;
            } else if (dent instanceof File f) {
                f.setMode(mode);
                f.setMtime(System.currentTimeMillis());
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

            if (!(found.getNode().getMeta() instanceof JKleppmannTreeNodeMetaDirectory md))
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT);

            return found.getNode().getChildren().keySet();
        });
    }

    @Override
    public Optional<ByteString> read(JObjectKey fileUuid, long offset, int length) {
        return jObjectTxManager.executeTx(() -> {
            if (length < 0)
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("Length should be more than zero: " + length));
            if (offset < 0)
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("Offset should be more than zero: " + offset));

            var file = curTx.getObject(File.class, fileUuid).orElse(null);
            if (file == null) {
                Log.error("File not found when trying to read: " + fileUuid);
                return Optional.empty();
            }

            try {
                var chunksAll = file.getChunks();
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
        var chunkRead = curTx.getObject(ChunkData.class, uuid).orElse(null);

        if (chunkRead == null) {
            Log.error("Chunk requested not found: " + uuid);
            throw new StatusRuntimeException(Status.NOT_FOUND);
        }

        return chunkRead.getData();
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
            var file = curTx.getObject(File.class, fileUuid).orElse(null);
            if (file == null) {
                Log.error("File not found when trying to write: " + fileUuid);
                return -1L;
            }

            if (writeLogging) {
                Log.info("Writing to file: " + file.getKey() + " size=" + size(fileUuid) + " "
                        + offset + " " + data.size());
            }

            if (size(fileUuid) < offset)
                truncate(fileUuid, offset);

            // FIXME: Some kind of immutable interface?
            var chunksAll = Collections.unmodifiableNavigableMap(file.getChunks());
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
                    newChunks.put(start, newChunkData.getKey());

                    start += thisChunk.size();
                    cur = end;
                }
            }

            file.setChunks(newChunks);
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

            var file = curTx.getObject(File.class, fileUuid).orElse(null);
            if (file == null) {
                Log.error("File not found when trying to write: " + fileUuid);
                return false;
            }

            if (length == 0) {
                var oldChunks = Collections.unmodifiableNavigableMap(new TreeMap<>(file.getChunks()));

                file.setChunks(new TreeMap<>());
                file.setMtime(System.currentTimeMillis());
                cleanupChunks(file, oldChunks.values());
                updateFileSize(file);
                return true;
            }

            var curSize = size(fileUuid);
            if (curSize == length) return true;

            var chunksAll = Collections.unmodifiableNavigableMap(file.getChunks());
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
                        newChunks.put(start, newChunkData.getKey());

                        start += newChunkData.getData().size();
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
                newChunks.put(tail.getKey(), newChunkData.getKey());
            }

            file.setChunks(newChunks);
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
            var fileOpt = curTx.getObject(File.class, uuid).orElseThrow(() -> new StatusRuntimeException(Status.NOT_FOUND.withDescription("File not found when trying to readlink: " + uuid)));
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

            File f = objectAllocator.create(File.class, new JObjectKey(fuuid.toString()));
            f.setSymlink(true);
            ChunkData newChunkData = createChunk(UnsafeByteOperations.unsafeWrap(oldpath.getBytes(StandardCharsets.UTF_8)));

            f.getChunks().put(0L, newChunkData.getKey());
            updateFileSize(f);

            getTree().move(parent.getKey(), new JKleppmannTreeNodeMetaFile(fname, f.getKey()), getTree().getNewNodeId());
            return f.getKey();
        });
    }

    @Override
    public Boolean setTimes(JObjectKey fileUuid, long atimeMs, long mtimeMs) {
        return jObjectTxManager.executeTx(() -> {
            var file = curTx.getObject(File.class, fileUuid).orElseThrow(
                    () -> new StatusRuntimeException(Status.NOT_FOUND.withDescription(
                            "File not found for setTimes: " + fileUuid))
            );

            file.setMtime(mtimeMs);
            return true;
        });
    }

    @Override
    public void updateFileSize(File file) {
        jObjectTxManager.executeTx(() -> {
            long realSize = 0;

            var last = file.getChunks().lastEntry();
            if (last != null) {
                var lastSize = getChunkSize(last.getValue());
                realSize = last.getKey() + lastSize;
            }

            if (realSize != file.getSize()) {
                file.setSize(realSize);
            }
        });
    }

    @Override
    public Long size(JObjectKey uuid) {
        return jObjectTxManager.executeTx(() -> {
            var read = curTx.getObject(File.class, uuid)
                    .orElseThrow(() -> new StatusRuntimeException(Status.NOT_FOUND));

            return read.getSize();
        });
    }
}
