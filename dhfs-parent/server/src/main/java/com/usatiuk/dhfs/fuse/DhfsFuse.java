package com.usatiuk.dhfs.fuse;

import com.google.protobuf.UnsafeByteOperations;
import com.sun.security.auth.module.UnixSystem;
import com.usatiuk.dhfs.files.service.DhfsFileService;
import com.usatiuk.dhfs.files.service.DirectoryNotEmptyException;
import com.usatiuk.dhfs.files.service.GetattrRes;
import com.usatiuk.dhfs.objects.repository.persistence.ObjectPersistentStore;
import com.usatiuk.kleppmanntree.AlreadyExistsException;
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
import org.eclipse.microprofile.config.inject.ConfigProperty;
import ru.serce.jnrfuse.ErrorCodes;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;
import ru.serce.jnrfuse.struct.Statvfs;
import ru.serce.jnrfuse.struct.Timespec;

import java.nio.file.Paths;
import java.util.Optional;

import static jnr.posix.FileStat.*;

@ApplicationScoped
public class DhfsFuse extends FuseStubFS {
    @Inject
    ObjectPersistentStore persistentStore; // FIXME?

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

    void init(@Observes @Priority(100000) StartupEvent event) {
        if (!enabled) return;
        Paths.get(root).toFile().mkdirs();
        Log.info("Mounting with root " + root);

        var uid = new UnixSystem().getUid();
        var gid = new UnixSystem().getGid();

        mount(Paths.get(root), false, debug,
              new String[]{"-o", "auto_cache", "-o", "uid=" + uid, "-o", "gid=" + gid});
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
            int target = targetChunkSize > 0 ? targetChunkSize : 4096;
            stbuf.f_frsize.set(target);
            stbuf.f_bsize.set(target);
            stbuf.f_blocks.set(persistentStore.getTotalSpace() / target); // total data blocks in file system
            stbuf.f_bfree.set(persistentStore.getFreeSpace() / target);  // free blocks in fs
            stbuf.f_bavail.set(persistentStore.getUsableSpace() / target); // avail blocks in fs
            stbuf.f_files.set(1000); //FIXME:
            stbuf.f_ffree.set(Integer.MAX_VALUE - 2000); //FIXME:
            stbuf.f_favail.set(Integer.MAX_VALUE - 2000); //FIXME:
            stbuf.f_namemax.set(2048);
            return super.statfs(path, stbuf);
        } catch (Exception e) {
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
        } catch (Exception e) {
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
        } catch (Exception e) {
            Log.error("When utimens " + path, e);
            return -ErrorCodes.EIO();
        }
    }

    @Override
    public int open(String path, FuseFileInfo fi) {
        try {
            if (fileService.open(path).isEmpty()) return -ErrorCodes.ENOENT();
            return 0;
        } catch (Exception e) {
            Log.error("When open " + path, e);
            return -ErrorCodes.EIO();
        }
    }

    @Override
    public int read(String path, Pointer buf, long size, long offset, FuseFileInfo fi) {
        if (size < 0) return -ErrorCodes.EINVAL();
        if (offset < 0) return -ErrorCodes.EINVAL();
        try {
            var fileOpt = fileService.open(path);
            if (fileOpt.isEmpty()) return -ErrorCodes.ENOENT();
            var file = fileOpt.get();
            var read = fileService.read(fileOpt.get(), offset, (int) size);
            if (read.isEmpty()) return 0;
            UnsafeByteOperations.unsafeWriteTo(read.get(), new JnrPtrByteOutput(jnrPtrByteOutputAccessors, buf, size));
            return read.get().size();
        } catch (Exception e) {
            Log.error("When reading " + path, e);
            return -ErrorCodes.EIO();
        }
    }

    @Override
    public int write(String path, Pointer buf, long size, long offset, FuseFileInfo fi) {
        if (offset < 0) return -ErrorCodes.EINVAL();
        try {
            var fileOpt = fileService.open(path);
            if (fileOpt.isEmpty()) return -ErrorCodes.ENOENT();
            byte[] buffer = new byte[(int) size];
            buf.get(0, buffer, 0, (int) size);
            var written = fileService.write(fileOpt.get(), offset, buffer);
            return written.intValue();
        } catch (Exception e) {
            Log.error("When writing " + path, e);
            return -ErrorCodes.EIO();
        }
    }

    @Override
    public int create(String path, long mode, FuseFileInfo fi) {
        try {
            var ret = fileService.create(path, mode);
            if (ret.isEmpty()) return -ErrorCodes.ENOSPC();
            else return 0;
        } catch (Exception e) {
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
        } catch (Exception e) {
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
        } catch (Exception e) {
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
        } catch (Exception e) {
            Log.error("When renaming " + path, e);
            return -ErrorCodes.EIO();
        }

    }

    @Override
    public int unlink(String path) {
        try {
            fileService.unlink(path);
            return 0;
        } catch (Exception e) {
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
        } catch (Exception e) {
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
        } catch (Exception e) {
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
        } catch (Exception e) {
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
        } catch (Exception e) {
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
        } catch (Exception e) {
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
        } catch (Exception e) {
            Log.error("When creating " + newpath, e);
            return -ErrorCodes.EIO();
        }
    }
}
