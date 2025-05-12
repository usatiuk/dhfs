package com.usatiuk.dhfsfs.service;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import com.usatiuk.dhfs.jkleppmanntree.JKleppmannTreeManager;
import com.usatiuk.dhfs.jkleppmanntree.structs.JKleppmannTreeNode;
import com.usatiuk.dhfs.jkleppmanntree.structs.JKleppmannTreeNodeHolder;
import com.usatiuk.dhfs.jkleppmanntree.structs.JKleppmannTreeNodeMeta;
import com.usatiuk.dhfs.jmap.JMapEntry;
import com.usatiuk.dhfs.jmap.JMapHelper;
import com.usatiuk.dhfs.jmap.JMapLongKey;
import com.usatiuk.dhfs.remoteobj.JDataRemote;
import com.usatiuk.dhfs.remoteobj.RemoteObjectMeta;
import com.usatiuk.dhfs.remoteobj.RemoteTransaction;
import com.usatiuk.dhfsfs.objects.ChunkData;
import com.usatiuk.dhfsfs.objects.File;
import com.usatiuk.dhfsfs.objects.JKleppmannTreeNodeMetaDirectory;
import com.usatiuk.dhfsfs.objects.JKleppmannTreeNodeMetaFile;
import com.usatiuk.objects.JData;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.objects.iterators.IteratorStart;
import com.usatiuk.objects.transaction.Transaction;
import com.usatiuk.objects.transaction.TransactionManager;
import com.usatiuk.utils.StatusRuntimeExceptionNoStacktrace;
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

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.StreamSupport;

/**
 * Actual filesystem implementation.
 */
@ApplicationScoped
public class DhfsFileService {
    @ConfigProperty(name = "dhfs.files.target_chunk_alignment")
    int targetChunkAlignment;
    @ConfigProperty(name = "dhfs.files.target_chunk_size")
    int targetChunkSize;
    @ConfigProperty(name = "dhfs.files.max_chunk_size", defaultValue = "524288")
    int maxChunkSize;
    @ConfigProperty(name = "dhfs.files.use_hash_for_chunks")
    boolean useHashForChunks;
    @ConfigProperty(name = "dhfs.files.allow_recursive_delete")
    boolean allowRecursiveDelete;
    @ConfigProperty(name = "dhfs.objects.ref_verification")
    boolean refVerification;
    @ConfigProperty(name = "dhfs.objects.write_log")
    boolean writeLogging;

    @Inject
    Transaction curTx;
    @Inject
    RemoteTransaction remoteTx;
    @Inject
    TransactionManager jObjectTxManager;
    @Inject
    JKleppmannTreeManager jKleppmannTreeManager;
    @Inject
    JMapHelper jMapHelper;

    private JKleppmannTreeManager.JKleppmannTree getTree() {
        return jKleppmannTreeManager.getTree(JObjectKey.of("fs"), () -> new JKleppmannTreeNodeMetaDirectory(""));
    }

    /**
     * Create a new chunk with the given data and a new unique ID.
     *
     * @param bytes the data to store in the chunk
     * @return the created chunk
     */
    private ChunkData createChunk(ByteString bytes) {
        var newChunk = new ChunkData(JObjectKey.of(UUID.randomUUID().toString()), bytes);
        remoteTx.putDataNew(newChunk);
        return newChunk;
    }

    void init(@Observes @Priority(500) StartupEvent event) {
        Log.info("Initializing file service");
        getTree();
    }

    private JKleppmannTreeNode getDirEntry(String name) {
        var res = getTree().traverse(StreamSupport.stream(Path.of(name).spliterator(), false).map(p -> p.toString()).toList());
        if (res == null) throw new StatusRuntimeExceptionNoStacktrace(Status.NOT_FOUND);
        var ret = curTx.get(JKleppmannTreeNodeHolder.class, res).map(JKleppmannTreeNodeHolder::node).orElseThrow(() -> new StatusRuntimeException(Status.NOT_FOUND.withDescription("Tree node exists but not found as jObject: " + name)));
        return ret;
    }

    private Optional<JKleppmannTreeNode> getDirEntryOpt(String name) {
        var res = getTree().traverse(StreamSupport.stream(Path.of(name).spliterator(), false).map(p -> p.toString()).toList());
        if (res == null) return Optional.empty();
        var ret = curTx.get(JKleppmannTreeNodeHolder.class, res).map(JKleppmannTreeNodeHolder::node);
        return ret;
    }

    /**
     * Get the attributes of a file or directory.
     * @param uuid the UUID of the file or directory
     * @return the attributes of the file or directory
     */
    public Optional<GetattrRes> getattr(JObjectKey uuid) {
        return jObjectTxManager.executeTx(() -> {
            var ref = curTx.get(JData.class, uuid).orElse(null);
            if (ref == null) return Optional.empty();
            GetattrRes ret;
            if (ref instanceof RemoteObjectMeta r) {
                var remote = remoteTx.getData(JDataRemote.class, uuid).orElse(null);
                if (remote instanceof File f) {
                    ret = new GetattrRes(f.mTime(), f.cTime(), f.mode(), f.symlink() ? GetattrType.SYMLINK : GetattrType.FILE);
                } else {
                    throw new StatusRuntimeException(Status.DATA_LOSS.withDescription("FsNode is not an FsNode: " + ref.key()));
                }
            } else if (ref instanceof JKleppmannTreeNodeHolder) {
                ret = new GetattrRes(100, 100, 0700, GetattrType.DIRECTORY);
            } else {
                throw new StatusRuntimeException(Status.DATA_LOSS.withDescription("FsNode is not an FsNode: " + ref.key()));
            }
            return Optional.of(ret);
        });
    }

    /**
     * Try to resolve a path to a file or directory.
     * @param name the path to resolve
     * @return the key of the file or directory, or an empty optional if it does not exist
     */
    public Optional<JObjectKey> open(String name) {
        return jObjectTxManager.executeTx(() -> {
            try {
                var ret = getDirEntry(name);
                return switch (ret.meta()) {
                    case JKleppmannTreeNodeMetaFile f -> Optional.of(f.fileIno());
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

    /**
     * Create a new file with the given name and mode.
     * @param name the name of the file
     * @param mode the mode of the file
     * @return the key of the created file
     */
    public Optional<JObjectKey> create(String name, long mode) {
        return jObjectTxManager.executeTx(() -> {
            Path path = Path.of(name);
            var parent = getDirEntry(path.getParent().toString());

            ensureDir(parent);

            String fname = path.getFileName().toString();

            var fuuid = UUID.randomUUID();
            Log.debug("Creating file " + fuuid);
            File f = new File(JObjectKey.of(fuuid.toString()), mode, System.currentTimeMillis(), System.currentTimeMillis(), false);
            remoteTx.putData(f);

            try {
                getTree().move(parent.key(), new JKleppmannTreeNodeMetaFile(fname, f.key()), getTree().getNewNodeId());
            } catch (Exception e) {
//                fobj.getMeta().removeRef(newNodeId);
                throw e;
            }
            return Optional.of(f.key());
        });
    }

    /**
     * Get the parent directory of a file or directory.
     * @param ino the key of the file or directory
     * @return the parent directory
     */
    public Pair<String, JObjectKey> inoToParent(JObjectKey ino) {
        return jObjectTxManager.executeTx(() -> {
            // FIXME: Slow
            return getTree().findParent(w -> {
                if (w.meta() instanceof JKleppmannTreeNodeMetaFile f)
                    return f.fileIno().equals(ino);
                return false;
            });
        });
    }

    /**
     * Create a new directory with the given name and mode.
     * @param name the name of the directory
     * @param mode the mode of the directory
     */
    public void mkdir(String name, long mode) {
        jObjectTxManager.executeTx(() -> {
            Path path = Path.of(name);
            var parent = getDirEntry(path.getParent().toString());
            ensureDir(parent);

            String dname = path.getFileName().toString();

            Log.debug("Creating directory " + name);

            // TODO: No modes for directories yet
            getTree().move(parent.key(), new JKleppmannTreeNodeMetaDirectory(dname), getTree().getNewNodeId());
        });
    }

    /**
     * Unlink a file or directory.
     * @param name the name of the file or directory
     * @throws DirectoryNotEmptyException if the directory is not empty and recursive delete is not allowed
     */
    public void unlink(String name) {
        jObjectTxManager.executeTx(() -> {
            var node = getDirEntryOpt(name).orElse(null);
            if (node == null)
                throw new StatusRuntimeException(Status.NOT_FOUND.withDescription("File not found when trying to unlink: " + name));
            if (node.meta() instanceof JKleppmannTreeNodeMetaDirectory f) {
                if (!allowRecursiveDelete && !node.children().isEmpty())
                    throw new DirectoryNotEmptyException();
            }
            getTree().trash(node.meta(), node.key());
        });
    }

    /**
     * Rename a file or directory.
     * @param from the old name
     * @param to the new name
     * @return true if the rename was successful, false otherwise
     */
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

    /**
     * Change the mode of a file or directory.
     * @param uuid the ID of the file or directory
     * @param mode the new mode
     * @return true if the mode was changed successfully, false otherwise
     */
    public Boolean chmod(JObjectKey uuid, long mode) {
        return jObjectTxManager.executeTx(() -> {
            var dent = curTx.get(JData.class, uuid).orElseThrow(() -> new StatusRuntimeExceptionNoStacktrace(Status.NOT_FOUND));

            if (dent instanceof JKleppmannTreeNodeHolder) {
                return true;
            } else if (dent instanceof RemoteObjectMeta) {
                var remote = remoteTx.getData(JDataRemote.class, uuid).orElse(null);
                if (remote instanceof File f) {
                    remoteTx.putData(f.withMode(mode).withCurrentMTime());
                    return true;
                } else {
                    throw new IllegalArgumentException(uuid + " is not a file");
                }
            } else {
                throw new IllegalArgumentException(uuid + " is not a file");
            }
        });
    }

    /**
     * Read the contents of a directory.
     * @param name the path of the directory
     * @return an iterable of the names of the files in the directory
     */
    public Iterable<String> readDir(String name) {
        return jObjectTxManager.executeTx(() -> {
            var found = getDirEntry(name);

            if (!(found.meta() instanceof JKleppmannTreeNodeMetaDirectory md))
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT);

            return found.children().keySet();
        });
    }

    /**
     * Read the contents of a file.
     * @param fileUuid the ID of the file
     * @param offset the offset to start reading from
     * @param length the number of bytes to read
     * @return the contents of the file as a ByteString
     */
    public ByteString read(JObjectKey fileUuid, long offset, int length) {
        return jObjectTxManager.executeTx(() -> {
            if (length < 0)
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("Length should be more than zero: " + length));
            if (offset < 0)
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("Offset should be more than zero: " + offset));

            var file = remoteTx.getData(File.class, fileUuid).orElse(null);
            if (file == null) {
                Log.error("File not found when trying to read: " + fileUuid);
                throw new StatusRuntimeException(Status.NOT_FOUND.withDescription("File not found when trying to read: " + fileUuid));
            }

            try (var it = jMapHelper.getIterator(file, IteratorStart.LE, JMapLongKey.of(offset))) {
                if (!it.hasNext())
                    return ByteString.empty();

//                if (it.peekNextKey().key() != offset) {
//                    Log.warnv("Read over the end of file: {0} {1} {2}, next chunk: {3}", fileUuid, offset, length, it.peekNextKey());
//                    return Optional.of(ByteString.empty());
//                }
                long curPos = offset;
                ByteString buf = ByteString.empty();

                var chunk = it.next();

                while (curPos < offset + length) {
                    var chunkPos = chunk.getKey().key();

                    long offInChunk = curPos - chunkPos;

                    long toReadInChunk = (offset + length) - curPos;

                    var chunkBytes = readChunk(chunk.getValue().ref());

                    long readableLen = chunkBytes.size() - offInChunk;

                    var toReadReally = Math.min(readableLen, toReadInChunk);

                    if (toReadReally < 0) break;

                    buf = buf.concat(chunkBytes.substring((int) offInChunk, (int) (offInChunk + toReadReally)));

                    curPos += toReadReally;

                    if (readableLen > toReadInChunk)
                        break;

                    if (!it.hasNext()) break;

                    chunk = it.next();
                }

                return buf;
            } catch (Exception e) {
                Log.error("Error reading file: " + fileUuid, e);
                throw new StatusRuntimeException(Status.INTERNAL.withDescription("Error reading file: " + fileUuid));
            }
        });
    }

    /**
     * Get the size of a file.
     * @param uuid the ID of the file
     * @return the size of the file
     */
    private ByteString readChunk(JObjectKey uuid) {
        var chunkRead = remoteTx.getData(ChunkData.class, uuid).orElse(null);

        if (chunkRead == null) {
            Log.error("Chunk requested not found: " + uuid);
            throw new StatusRuntimeException(Status.NOT_FOUND);
        }

        return chunkRead.data();
    }

    /**
     * Get the size of a chunk.
     * @param uuid the ID of the chunk
     * @return the size of the chunk
     */
    private int getChunkSize(JObjectKey uuid) {
        return readChunk(uuid).size();
    }

    private long alignDown(long num, long n) {
        return num & -(1L << n);
    }

    /**
     * Write data to a file.
     * @param fileUuid the ID of the file
     * @param offset the offset to write to
     * @param data the data to write
     * @return the number of bytes written
     */
    public Long write(JObjectKey fileUuid, long offset, ByteString data) {
        return jObjectTxManager.executeTx(() -> {
            if (offset < 0)
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("Offset should be more than zero: " + offset));

            var file = remoteTx.getData(File.class, fileUuid).orElse(null);
            if (file == null) {
                throw new StatusRuntimeException(Status.NOT_FOUND.withDescription("File not found when trying to write: " + fileUuid));
            }

            Map<Long, JObjectKey> removedChunks = new HashMap<>();

            long realOffset = targetChunkAlignment >= 0 ? alignDown(offset, targetChunkAlignment) : offset;
            long writeEnd = offset + data.size();
            long start = realOffset;
            long existingEnd = 0;
            ByteString pendingPrefix = ByteString.empty();
            ByteString pendingSuffix = ByteString.empty();

            try (var it = jMapHelper.getIterator(file, IteratorStart.LE, JMapLongKey.of(realOffset))) {
                while (it.hasNext()) {
                    var curEntry = it.next();
                    long curChunkStart = curEntry.getKey().key();
                    var curChunkId = curEntry.getValue().ref();
                    long curChunkEnd = it.hasNext() ? it.peekNextKey().key() : curChunkStart + getChunkSize(curChunkId);
                    existingEnd = curChunkEnd;
                    if (curChunkEnd <= realOffset) break;

                    removedChunks.put(curEntry.getKey().key(), curChunkId);

                    if (curChunkStart < offset) {
                        if (curChunkStart < start)
                            start = curChunkStart;

                        var readChunk = readChunk(curChunkId);
                        pendingPrefix = pendingPrefix.concat(readChunk.substring(0, Math.min(readChunk.size(), (int) (offset - curChunkStart))));
                    }

                    if (curChunkEnd > writeEnd) {
                        var readChunk = readChunk(curChunkId);
                        pendingSuffix = pendingSuffix.concat(readChunk.substring((int) (writeEnd - curChunkStart), readChunk.size()));
                    }

                    if (curChunkEnd >= writeEnd) break;
                }
            }


            Map<Long, JObjectKey> newChunks = new HashMap<>();

            if (existingEnd < offset) {
                if (!pendingPrefix.isEmpty()) {
                    int diff = Math.toIntExact(offset - existingEnd);
                    pendingPrefix = pendingPrefix.concat(UnsafeByteOperations.unsafeWrap(ByteBuffer.allocateDirect(diff)));
                } else {
                    fillZeros(existingEnd, offset, newChunks);
                    start = offset;
                }
            }

            ByteString pendingWrites = pendingPrefix.concat(data).concat(pendingSuffix);

            int combinedSize = pendingWrites.size();

            {
                int cur = 0;
                while (cur < combinedSize) {
                    int end;

                    if (combinedSize - cur < maxChunkSize)
                        end = combinedSize;
                    else if (targetChunkAlignment < 0)
                        end = combinedSize;
                    else
                        end = Math.min(cur + targetChunkSize, combinedSize);

                    var thisChunk = pendingWrites.substring(cur, end);

                    ChunkData newChunkData = createChunk(thisChunk);
                    newChunks.put(start, newChunkData.key());

                    start += thisChunk.size();
                    cur = end;
                }
            }

            for (var e : removedChunks.entrySet()) {
//                Log.tracev("Removing chunk {0}-{1}", e.getKey(), e.getValue());
                jMapHelper.delete(file, JMapLongKey.of(e.getKey()));
            }

            for (var e : newChunks.entrySet()) {
//                Log.tracev("Adding chunk {0}-{1}", e.getKey(), e.getValue());
                jMapHelper.put(file, JMapLongKey.of(e.getKey()), e.getValue());
            }

            remoteTx.putData(file.withCurrentMTime());

            return (long) data.size();
        });
    }

    /**
     * Truncate a file to the given length.
     * @param fileUuid the ID of the file
     * @param length the new length of the file
     * @return true if the truncate was successful, false otherwise
     */
    public Boolean truncate(JObjectKey fileUuid, long length) {
        return jObjectTxManager.executeTx(() -> {
            if (length < 0)
                throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("Length should be more than zero: " + length));

            var file = remoteTx.getData(File.class, fileUuid).orElse(null);
            if (file == null) {
                Log.error("File not found when trying to write: " + fileUuid);
                return false;
            }

            if (length == 0) {
                jMapHelper.deleteAll(file);
                remoteTx.putData(file);
                return true;
            }

            var curSize = size(fileUuid);
            if (curSize == length) return true;

            NavigableMap<Long, JObjectKey> removedChunks = new TreeMap<>();
            NavigableMap<Long, JObjectKey> newChunks = new TreeMap<>();

            if (curSize < length) {
                fillZeros(curSize, length, newChunks);
            } else {
//                Pair<JMapLongKey, JMapEntry<JMapLongKey>> first;
                Pair<JMapLongKey, JMapEntry<JMapLongKey>> last;
                try (var it = jMapHelper.getIterator(file, IteratorStart.LT, JMapLongKey.of(length))) {
                    last = it.hasNext() ? it.next() : null;
                    while (it.hasNext()) {
                        var next = it.next();
                        removedChunks.put(next.getKey().key(), next.getValue().ref());
                    }
                }
                removedChunks.put(last.getKey().key(), last.getValue().ref());
//
//                NavigableMap<Long, JObjectKey> removedChunks = new TreeMap<>();
//
//                long start = 0;
//
//                try (var it = jMapHelper.getIterator(file, IteratorStart.LE, JMapLongKey.of(offset))) {
//                    first = it.hasNext() ? it.next() : null;
//                    boolean empty = last == null;
//                    if (first != null && getChunkSize(first.getValue().ref()) + first.getKey().key() <= offset) {
//                        first = null;
//                        last = null;
//                        start = offset;
//                    } else if (!empty) {
//                        assert first != null;
//                        removedChunks.put(first.getKey().key(), first.getValue().ref());
//                        while (it.hasNext() && it.peekNextKey() != last.getKey()) {
//                            var next = it.next();
//                            removedChunks.put(next.getKey().key(), next.getValue().ref());
//                        }
//                        removedChunks.put(last.getKey().key(), last.getValue().ref());
//                    }
//                }
//
//                var tail = chunksAll.lowerEntry(length);
//                var afterTail = chunksAll.tailMap(tail.getKey(), false);
//
//                removedChunks.put(tail.getKey(), tail.getValue());
//                removedChunks.putAll(afterTail);

                var tailBytes = readChunk(last.getValue().ref());
                var newChunk = tailBytes.substring(0, (int) (length - last.getKey().key()));

                ChunkData newChunkData = createChunk(newChunk);
                newChunks.put(last.getKey().key(), newChunkData.key());
            }

//            file = file.withChunks(file.chunks().minusAll(removedChunks.keySet()).plusAll(newChunks)).withMTime(System.currentTimeMillis());

            for (var e : removedChunks.entrySet()) {
//                Log.tracev("Removing chunk {0}-{1}", e.getKey(), e.getValue());
                jMapHelper.delete(file, JMapLongKey.of(e.getKey()));
            }

            for (var e : newChunks.entrySet()) {
//                Log.tracev("Adding chunk {0}-{1}", e.getKey(), e.getValue());
                jMapHelper.put(file, JMapLongKey.of(e.getKey()), e.getValue());
            }

            remoteTx.putData(file.withCurrentMTime());
            return true;
        });
    }

    /**
     * Fill the given range with zeroes.
     * @param fillStart the start of the range
     * @param length the end of the range
     * @param newChunks the map to store the new chunks in
     */
    private void fillZeros(long fillStart, long length, Map<Long, JObjectKey> newChunks) {
        long combinedSize = (length - fillStart);

        long start = fillStart;

        // Hack
        HashMap<Long, ChunkData> zeroCache = new HashMap<>();

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
                    zeroCache.put(end - cur, createChunk(UnsafeByteOperations.unsafeWrap(ByteBuffer.allocateDirect(Math.toIntExact(end - cur)))));

                ChunkData newChunkData = zeroCache.get(end - cur);
                newChunks.put(start, newChunkData.key());

                start += newChunkData.data().size();
                cur = end;
            }
        }
    }

    /**
     * Read the contents of a symlink.
     * @param uuid the ID of the symlink
     * @return the contents of the symlink as a string
     */
    public String readlink(JObjectKey uuid) {
        return jObjectTxManager.executeTx(() -> {
            return readlinkBS(uuid).toStringUtf8();
        });
    }

    /**
     * Read the contents of a symlink as a ByteString.
     * @param uuid the ID of the symlink
     * @return the contents of the symlink as a ByteString
     */
    public ByteString readlinkBS(JObjectKey uuid) {
        return jObjectTxManager.executeTx(() -> {
            var fileOpt = remoteTx.getData(File.class, uuid).orElseThrow(() -> new StatusRuntimeException(Status.NOT_FOUND.withDescription("File not found when trying to readlink: " + uuid)));
            return read(uuid, 0, Math.toIntExact(size(uuid)));
        });
    }

    /**
     * Create a symlink.
     * @param oldpath the target of the symlink
     * @param newpath the path of the symlink
     * @return the key of the created symlink
     */
    public JObjectKey symlink(String oldpath, String newpath) {
        return jObjectTxManager.executeTx(() -> {
            Path path = Path.of(newpath);
            var parent = getDirEntry(path.getParent().toString());

            ensureDir(parent);

            String fname = path.getFileName().toString();

            var fuuid = UUID.randomUUID();
            Log.debug("Creating file " + fuuid);

            ChunkData newChunkData = createChunk(UnsafeByteOperations.unsafeWrap(oldpath.getBytes(StandardCharsets.UTF_8)));
            File f = new File(JObjectKey.of(fuuid.toString()), 0, System.currentTimeMillis(), System.currentTimeMillis(), true);
            jMapHelper.put(f, JMapLongKey.of(0), newChunkData.key());

            remoteTx.putData(f);
            getTree().move(parent.key(), new JKleppmannTreeNodeMetaFile(fname, f.key()), getTree().getNewNodeId());
            return f.key();
        });
    }

    /**
     * Set the access and modification times of a file.
     * @param fileUuid the ID of the file
     * @param atimeMs the access time in milliseconds
     * @param mtimeMs the modification time in milliseconds
     * @return true if the times were set successfully, false otherwise
     */
    public Boolean setTimes(JObjectKey fileUuid, long atimeMs, long mtimeMs) {
        return jObjectTxManager.executeTx(() -> {
            var dent = curTx.get(JData.class, fileUuid).orElseThrow(() -> new StatusRuntimeExceptionNoStacktrace(Status.NOT_FOUND));

            // FIXME:
            if (dent instanceof JKleppmannTreeNodeHolder) {
                return true;
            } else if (dent instanceof RemoteObjectMeta) {
                var remote = remoteTx.getData(JDataRemote.class, fileUuid).orElse(null);
                if (remote instanceof File f) {
                    remoteTx.putData(f.withCTime(atimeMs).withMTime(mtimeMs));
                    return true;
                } else {
                    throw new IllegalArgumentException(fileUuid + " is not a file");
                }
            } else {
                throw new IllegalArgumentException(fileUuid + " is not a file");
            }
        });
    }

    /**
     * Get the size of a file.
     * @param fileUuid the ID of the file
     * @return the size of the file
     */
    public long size(JObjectKey fileUuid) {
        return jObjectTxManager.executeTx(() -> {
            long realSize = 0;
            var file = remoteTx.getData(File.class, fileUuid)
                    .orElseThrow(() -> new StatusRuntimeException(Status.NOT_FOUND));

            Pair<JMapLongKey, JMapEntry<JMapLongKey>> last;
            try (var it = jMapHelper.getIterator(file, IteratorStart.LT, JMapLongKey.max())) {
                last = it.hasNext() ? it.next() : null;
            }

            if (last != null) {
                realSize = last.getKey().key() + getChunkSize(last.getValue().ref());
            }

            return realSize;
        });
    }

    /**
     * Write data to a file.
     * @param fileUuid the ID of the file
     * @param offset the offset to write to
     * @param data the data to write
     * @return the number of bytes written
     */
    public Long write(JObjectKey fileUuid, long offset, byte[] data) {
        return write(fileUuid, offset, UnsafeByteOperations.unsafeWrap(data));
    }
}
