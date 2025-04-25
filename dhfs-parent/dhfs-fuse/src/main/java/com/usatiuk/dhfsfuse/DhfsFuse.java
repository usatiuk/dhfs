package com.usatiuk.dhfsfuse;

import com.google.protobuf.UnsafeByteOperations;
import com.sun.security.auth.module.UnixSystem;
import com.usatiuk.dhfsfs.service.DhfsFileService;
import com.usatiuk.dhfsfs.service.DirectoryNotEmptyException;
import com.usatiuk.dhfsfs.service.GetattrRes;
import com.usatiuk.kleppmanntree.AlreadyExistsException;
import com.usatiuk.objects.JObjectKey;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.logging.Log;
import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import jnr.ffi.Pointer;
import org.apache.commons.lang3.SystemUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;
import ru.serce.jnrfuse.struct.Statvfs;
import ru.serce.jnrfuse.struct.Timespec;

import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static jnr.posix.FileStat.*;

@ApplicationScoped
public class DhfsFuse extends FuseStubFS {
    private static final int blksize = 1048576;
    private static final int iosize = 1048576;
    private final ConcurrentHashMap<Long, JObjectKey> _openHandles = new ConcurrentHashMap<>();
    private final AtomicLong _fh = new AtomicLong(1);
    @ConfigProperty(name = "dhfs.fuse.root")
    String root;
    @ConfigProperty(name = "dhfs.fuse.enabled")
    boolean enabled;
    @ConfigProperty(name = "dhfs.fuse.debug")
    Boolean debug;
    @ConfigProperty(name = "dhfs.files.target_chunk_size")
    int targetChunkSize;
    @Inject
    JnrPtrByteOutputAccessors jnrPtrByteOutputAccessors;
    @Inject
    DhfsFileService fileService;

    private long allocateHandle(JObjectKey key) {
        while (true) {
            var newFh = _fh.getAndIncrement();
            if (newFh == 0) continue;
            if (_openHandles.putIfAbsent(newFh, key) == null) {
                return newFh;
            }
        }
    }

    private JObjectKey getFromHandle(long handle) {
        assert handle != 0;
        return _openHandles.get(handle);
    }

    void init(@Observes @Priority(100000) StartupEvent event) {
        if (!enabled) return;
        Paths.get(root).toFile().mkdirs();

        if (!Paths.get(root).toFile().isDirectory())
            throw new IllegalStateException("Could not create directory " + root);

        Log.info("Mounting with root " + root);

        var uid = new UnixSystem().getUid();
        var gid = new UnixSystem().getGid();

        var opts = new ArrayList<String>();

        // Assuming macFuse
        if (SystemUtils.IS_OS_MAC) {
            opts.add("-o");
            opts.add("iosize=" + iosize);
        } else if (SystemUtils.IS_OS_LINUX) {
            // FIXME: There's something else missing: the writes still seem to be 32k max
//            opts.add("-o");
//            opts.add("large_read");
            opts.add("-o");
            opts.add("big_writes");
            opts.add("-o");
            opts.add("max_read=" + iosize);
            opts.add("-o");
            opts.add("max_write=" + iosize);
        }
        opts.add("-o");
        opts.add("auto_cache");
        opts.add("-o");
        opts.add("uid=" + uid);
        opts.add("-o");
        opts.add("gid=" + gid);

        mount(Paths.get(root), false, debug, opts.toArray(String[]::new));
    }

    void shutdown(@Observes @Priority(1) ShutdownEvent event) {
        if (!enabled) return;
        Log.info("Unmounting");
        umount();
        Log.info("Unmounted");
    }

    @Override
    public int statfs(String path, Statvfs stbuf) {
        try {
            stbuf.f_frsize.set(blksize);
            stbuf.f_bsize.set(blksize);
            // FIXME:
            stbuf.f_blocks.set(1024 * 1024 * 1024 / blksize); // total data blocks in file system
            stbuf.f_bfree.set(1024 * 1024 * 1024 / blksize);  // free blocks in fs
            stbuf.f_bavail.set(1024 * 1024 * 1024 / blksize); // avail blocks in fs
            stbuf.f_files.set(1000); //FIXME:
            stbuf.f_ffree.set(Integer.MAX_VALUE - 2000); //FIXME:
            stbuf.f_favail.set(Integer.MAX_VALUE - 2000); //FIXME:
            stbuf.f_namemax.set(2048);
            return super.statfs(path, stbuf);
        } catch (Throwable e) {
            Log.error("When statfs " + path, e);
            return -ErrorCodes.EIO();
        }
    }

    @Override
    public int getattr(String path, FileStat stat) {
        try {
            var fileOpt = fileService.open(path);
            if (fileOpt.isEmpty()) return -ErrorCodes.ENOENT();
            var uuid = fileOpt.get();
            Optional<GetattrRes> found = fileService.getattr(uuid);
            if (found.isEmpty()) {
                return -ErrorCodes.ENOENT();
            }
            switch (found.get().type()) {
                case FILE -> {
                    stat.st_mode.set(S_IFREG | found.get().mode());
                    stat.st_nlink.set(1);
                    stat.st_size.set(fileService.size(uuid));
                }
                case DIRECTORY -> {
                    stat.st_mode.set(S_IFDIR | found.get().mode());
                    stat.st_nlink.set(2);
                }
                case SYMLINK -> {
                    stat.st_mode.set(S_IFLNK | 0777);
                    stat.st_nlink.set(1);
                    stat.st_size.set(fileService.size(uuid));
                }
            }

            // FIXME: Race?
            stat.st_ctim.tv_sec.set(found.get().ctime() / 1000);
            stat.st_ctim.tv_nsec.set((found.get().ctime() % 1000) * 1000);
            stat.st_mtim.tv_sec.set(found.get().mtime() / 1000);
            stat.st_mtim.tv_nsec.set((found.get().mtime() % 1000) * 1000);
            stat.st_atim.tv_sec.set(found.get().mtime() / 1000);
            stat.st_atim.tv_nsec.set((found.get().mtime() % 1000) * 1000);
            stat.st_blksize.set(blksize);
        } catch (Throwable e) {
            Log.error("When getattr " + path, e);
            return -ErrorCodes.EIO();
        }
        return 0;
    }

    @Override
    public int utimens(String path, Timespec[] timespec) {
        try {
            var fileOpt = fileService.open(path);
            if (fileOpt.isEmpty()) return -ErrorCodes.ENOENT();
            var file = fileOpt.get();
            var res = fileService.setTimes(file,
                    timespec[0].tv_sec.get() * 1000,
                    timespec[1].tv_sec.get() * 1000);
            if (!res) return -ErrorCodes.EINVAL();
            else return 0;
        } catch (Throwable e) {
            Log.error("When utimens " + path, e);
            return -ErrorCodes.EIO();
        }
    }

    @Override
    public int open(String path, FuseFileInfo fi) {
        try {
            var opened = fileService.open(path);
            if (opened.isEmpty()) return -ErrorCodes.ENOENT();
            fi.fh.set(allocateHandle(opened.get()));
            return 0;
        } catch (Throwable e) {
            Log.error("When open " + path, e);
            return -ErrorCodes.EIO();
        }
    }

    @Override
    public int release(String path, FuseFileInfo fi) {
        assert fi.fh.get() != 0;
        _openHandles.remove(fi.fh.get());
        return 0;
    }

    @Override
    public int read(String path, Pointer buf, long size, long offset, FuseFileInfo fi) {
        if (size < 0) return -ErrorCodes.EINVAL();
        if (offset < 0) return -ErrorCodes.EINVAL();
        try {
            var fileKey = getFromHandle(fi.fh.get());
            var read = fileService.read(fileKey, offset, (int) size);
            if (read.isEmpty()) return 0;
            UnsafeByteOperations.unsafeWriteTo(read.get(), new JnrPtrByteOutput(jnrPtrByteOutputAccessors, buf, size));
            return read.get().size();
        } catch (Throwable e) {
            Log.error("When reading " + path, e);
            return -ErrorCodes.EIO();
        }
    }

    @Override
    public int write(String path, Pointer buf, long size, long offset, FuseFileInfo fi) {
        if (offset < 0) return -ErrorCodes.EINVAL();
        try {
            var fileKey = getFromHandle(fi.fh.get());
            var buffer = ByteBuffer.allocateDirect((int) size);

            if (buffer.isDirect()) {
                jnrPtrByteOutputAccessors.getUnsafe().copyMemory(
                        buf.address(),
                        jnrPtrByteOutputAccessors.getNioAccess().getBufferAddress(buffer),
                        size
                );
            } else {
                buf.get(0, buffer.array(), 0, (int) size);
            }

            var written = fileService.write(fileKey, offset, UnsafeByteOperations.unsafeWrap(buffer));
            return written.intValue();
        } catch (Throwable e) {
            Log.error("When writing " + path, e);
            return -ErrorCodes.EIO();
        }
    }

    @Override
    public int create(String path, long mode, FuseFileInfo fi) {
        try {
            var ret = fileService.create(path, mode);
            if (ret.isEmpty()) return -ErrorCodes.ENOSPC();
            fi.fh.set(allocateHandle(ret.get()));
            return 0;
        } catch (Throwable e) {
            Log.error("When creating " + path, e);
            return -ErrorCodes.EIO();
        }
    }

    @Override
    public int mkdir(String path, long mode) {
        try {
            fileService.mkdir(path, mode);
            return 0;
        } catch (AlreadyExistsException aex) {
            return -ErrorCodes.EEXIST();
        } catch (Throwable e) {
            Log.error("When creating dir " + path, e);
            return -ErrorCodes.EIO();
        }
    }

    @Override
    public int rmdir(String path) {
        try {
            fileService.unlink(path);
            return 0;
        } catch (DirectoryNotEmptyException ex) {
            return -ErrorCodes.ENOTEMPTY();
        } catch (Throwable e) {
            Log.error("When removing dir " + path, e);
            return -ErrorCodes.EIO();
        }
    }

    @Override
    public int rename(String path, String newName) {
        try {
            var ret = fileService.rename(path, newName);
            if (!ret) return -ErrorCodes.ENOENT();
            else return 0;
        } catch (Throwable e) {
            Log.error("When renaming " + path, e);
            return -ErrorCodes.EIO();
        }

    }

    @Override
    public int unlink(String path) {
        try {
            fileService.unlink(path);
            return 0;
        } catch (Throwable e) {
            Log.error("When unlinking " + path, e);
            return -ErrorCodes.EIO();
        }
    }

    @Override
    public int truncate(String path, long size) {
        if (size < 0) return -ErrorCodes.EINVAL();
        try {
            var fileOpt = fileService.open(path);
            if (fileOpt.isEmpty()) return -ErrorCodes.ENOENT();
            var file = fileOpt.get();
            var ok = fileService.truncate(file, size);
            if (ok)
                return 0;
            else
                return -ErrorCodes.ENOSPC();
        } catch (Throwable e) {
            Log.error("When truncating " + path, e);
            return -ErrorCodes.EIO();
        }
    }

    @Override
    public int chmod(String path, long mode) {
        try {
            var fileOpt = fileService.open(path);
            if (fileOpt.isEmpty()) return -ErrorCodes.ENOENT();
            var ret = fileService.chmod(fileOpt.get(), mode);
            if (ret) return 0;
            else return -ErrorCodes.EINVAL();
        } catch (Throwable e) {
            Log.error("When chmod " + path, e);
            return -ErrorCodes.EIO();
        }
    }

    @Override
    public int readdir(String path, Pointer buf, FuseFillDir filler, long offset, FuseFileInfo fi) {
        try {
            Iterable<String> found;
            try {
                found = fileService.readDir(path);
            } catch (StatusRuntimeException e) {
                if (e.getStatus().getCode().equals(Status.NOT_FOUND.getCode()))
                    return -ErrorCodes.ENOENT();
                else throw e;
            }

            filler.apply(buf, ".", null, 0);
            filler.apply(buf, "..", null, 0);

            for (var c : found) {
                filler.apply(buf, c, null, 0);
            }

            return 0;
        } catch (Throwable e) {
            Log.error("When readdir " + path, e);
            return -ErrorCodes.EIO();
        }
    }

    @Override
    public int readlink(String path, Pointer buf, long size) {
        if (size < 0) return -ErrorCodes.EINVAL();
        try {
            var fileOpt = fileService.open(path);
            if (fileOpt.isEmpty()) return -ErrorCodes.ENOENT();
            var file = fileOpt.get();
            var read = fileService.readlinkBS(fileOpt.get());
            if (read.isEmpty()) return 0;
            UnsafeByteOperations.unsafeWriteTo(read, new JnrPtrByteOutput(jnrPtrByteOutputAccessors, buf, size));
            buf.putByte(Math.min(size - 1, read.size()), (byte) 0);
            return 0;
        } catch (Throwable e) {
            Log.error("When reading " + path, e);
            return -ErrorCodes.EIO();
        }
    }

    @Override
    public int chown(String path, long uid, long gid) {
        try {
            var fileOpt = fileService.open(path);
            if (fileOpt.isEmpty()) return -ErrorCodes.ENOENT();
            return 0;
        } catch (Throwable e) {
            Log.error("When chown " + path, e);
            return -ErrorCodes.EIO();
        }
    }

    @Override
    public int symlink(String oldpath, String newpath) {
        try {
            var ret = fileService.symlink(oldpath, newpath);
            if (ret == null) return -ErrorCodes.EEXIST();
            else return 0;
        } catch (Throwable e) {
            Log.error("When creating " + newpath, e);
            return -ErrorCodes.EIO();
        }
    }
}
