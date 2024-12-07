package org.acme.files.service;

import com.google.protobuf.ByteString;
import com.google.protobuf.UnsafeByteOperations;
import com.usatiuk.dhfs.objects.TransactionManager;
import com.usatiuk.dhfs.objects.transaction.Transaction;
import com.usatiuk.objects.alloc.runtime.ObjectAllocator;
import com.usatiuk.objects.common.runtime.JObjectKey;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.logging.Log;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.acme.files.objects.ChunkData;
import org.acme.files.objects.File;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import java.util.*;

@ApplicationScoped
public class DhfsFileService {

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

    @ConfigProperty(name = "dhfs.objects.write_log")
    boolean writeLogging;

    @Inject
    Transaction curTx;

    @Inject
    TransactionManager txm;

    @Inject
    ObjectAllocator alloc;

    long chunkCounter = 0;

    void init(@Observes @Priority(500) StartupEvent event) {
        Log.info("Initializing file service");
    }

    public Optional<String> open(String path) {
        return txm.run(() -> {
            if (curTx.getObject(File.class, new JObjectKey(path)).orElse(null) != null) {
                return Optional.of(path);
            }
            return Optional.empty();
        });
    }

    public Optional<String> create(String path) {
        if (path.contains("/")) {
            throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("Path should not contain slashes"));
        }

        return txm.run(() -> {
            var file = alloc.create(File.class, new JObjectKey(path));
            curTx.putObject(file);
            return Optional.of(path);
        });
    }

    private JObjectKey createChunk(ByteString bytes) {
        var cd = alloc.create(ChunkData.class, new JObjectKey("chunk-" + chunkCounter++));
        cd.setData(bytes.toByteArray());
        curTx.putObject(cd);
        return cd.getKey();
    }

    private ByteString readChunk(JObjectKey uuid) {
        var chunk = curTx.getObject(ChunkData.class, uuid);
        if (chunk.isEmpty()) {
            Log.error("Chunk not found when trying to read: " + uuid);
            throw new StatusRuntimeException(Status.NOT_FOUND.withDescription("Chunk not found"));
        }
        return UnsafeByteOperations.unsafeWrap(chunk.get().getData());
    }

    private static final List<String> fileNames = List.of("file1", "file2");

    public List<String> readdir(String path) {
        if (!path.equals("")) {
            throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("Only root directory is supported"));
        }

        return txm.run(() -> {
            var ret = new ArrayList<String>();
            for (String fileName : fileNames) {
                var got = curTx.getObject(File.class, new JObjectKey(fileName));
                if (got.isPresent()) {
                    ret.add(fileName);
                }
            }
            return ret;
        });
    }

    public Optional<ByteString> read(String fileUuid, long offset, int length) {
        if (length < 0)
            throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("Length should be more than zero: " + length));
        if (offset < 0)
            throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("Offset should be more than zero: " + offset));

        return txm.run(() -> {
            var file = curTx.getObject(File.class, new JObjectKey(fileUuid)).orElse(null);
            if (file == null) {
                Log.error("File not found when trying to read: " + fileUuid);
                return Optional.empty();
            }

            try {
                var chunksAll = new TreeMap<>(file.getChunks());
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


    public Long write(String fileUuid, long offset, ByteString data) {
        if (offset < 0)
            throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("Offset should be more than zero: " + offset));

        return txm.run(() -> {
            // FIXME:
            var file = curTx.getObject(File.class, new JObjectKey(fileUuid)).orElse(null);
            if (file == null) {
                Log.error("File not found when trying to read: " + fileUuid);
                return -1L;
            }

            if (size(fileUuid) < offset)
                truncate(fileUuid, offset);

            // Get chunk ids from the database
            var chunksAll = file.getChunks();
            var first = chunksAll.floorEntry(offset);
            var last = chunksAll.lowerEntry(offset + data.size());
            NavigableMap<Long, JObjectKey> removedChunks = new TreeMap<>();

            long start = 0;

            NavigableMap<Long, JObjectKey> beforeFirst = first != null ? chunksAll.headMap(first.getKey(), false) : Collections.emptyNavigableMap();
            NavigableMap<Long, JObjectKey> afterLast = last != null ? chunksAll.tailMap(last.getKey(), false) : Collections.emptyNavigableMap();

            if (first != null && readChunk(first.getValue()).size() + first.getKey() <= offset) {
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

                            if (readChunk(cuuid).size() >= (targetChunkSize * writeMergeMaxChunkToTake)) {
                                leftDone = true;
                                continue;
                            }

                            if ((combinedSize + readChunk(cuuid).size()) > (targetChunkSize * writeMergeLimit)) {
                                leftDone = true;
                                continue;
                            }

                            // FIXME: (and test this)
                            beforeFirst = beforeFirst.headMap(takeLeft.getKey(), false);
                            start = takeLeft.getKey();
                            pendingWrites = readChunk(cuuid).concat(pendingWrites);
                            combinedSize += readChunk(cuuid).size();
                            removedChunks.put(takeLeft.getKey(), takeLeft.getValue());
                        }
                        if (afterLast.isEmpty()) rightDone = true;
                        if (!afterLast.isEmpty() && !rightDone) {
                            var takeRight = afterLast.firstEntry();

                            var cuuid = takeRight.getValue();

                            if (readChunk(cuuid).size() >= (targetChunkSize * writeMergeMaxChunkToTake)) {
                                rightDone = true;
                                continue;
                            }

                            if ((combinedSize + readChunk(cuuid).size()) > (targetChunkSize * writeMergeLimit)) {
                                rightDone = true;
                                continue;
                            }

                            // FIXME: (and test this)
                            afterLast = afterLast.tailMap(takeRight.getKey(), false);
                            pendingWrites = pendingWrites.concat(readChunk(cuuid));
                            combinedSize += readChunk(cuuid).size();
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

                    newChunks.put(start, createChunk(thisChunk));

                    start += thisChunk.size();
                    cur = end;
                }
            }

            var newChunksMap = new TreeMap<>(chunksAll);

            for (var e : removedChunks.entrySet()) {
                newChunksMap.remove(e.getKey());
//            em.remove(em.getReference(ChunkData.class, e.getValue()));
            }

            newChunksMap.putAll(newChunks);

            file.setChunks(newChunksMap);

            updateFileSize(file);

            return (long) data.size();
        });
    }

    public Boolean truncate(String fileUuid, long length) {
        if (length < 0)
            throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription("Length should be more than zero: " + length));

        return txm.run(() -> {
            var file = curTx.getObject(File.class, new JObjectKey(fileUuid)).orElse(null);
            if (file == null) {
                Log.error("File not found when trying to read: " + fileUuid);
                return false;
            }

            if (length == 0) {
                file.setChunks(new TreeMap<>());
                updateFileSize(file);
                return true;
            }

            var curSize = size(fileUuid);
            if (curSize == length) return true;

            var chunksAll = file.getChunks();
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

                        newChunks.put(start, createChunk(zeroCache.get(end - cur)));
                        start += (end - cur);
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

                newChunks.put(tail.getKey(), createChunk(newChunk));
            }

            var newChunkMap = new TreeMap<>(chunksAll);

            for (var e : removedChunks.entrySet()) {
                newChunkMap.remove(e.getKey());
//            em.remove(em.getReference(ChunkData.class, e.getValue()));
            }
            newChunkMap.putAll(newChunks);

            file.setChunks(newChunkMap);

            updateFileSize(file);
            return true;
        });
    }

    public void updateFileSize(File file) {
        long realSize = 0;

        var last = file.getChunks().lastEntry();
        if (last != null) {
            var lastSize = readChunk(last.getValue()).size();
            realSize = last.getKey() + lastSize;
        }

        if (realSize != file.getSize()) {
            file.setSize(realSize);
        }
    }

    public Long size(String uuid) {
        return txm.run(() -> {
            var file = curTx.getObject(File.class, new JObjectKey(uuid)).orElse(null);
            if (file == null) {
                Log.error("File not found when trying to read: " + uuid);
                return -1L;
            }
            return file.getSize();
        });
    }
}
