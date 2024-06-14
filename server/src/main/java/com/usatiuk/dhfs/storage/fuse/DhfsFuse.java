package com.usatiuk.dhfs.storage.fuse;

import com.sun.security.auth.module.UnixSystem;
import com.usatiuk.dhfs.storage.files.objects.DirEntry;
import com.usatiuk.dhfs.storage.files.objects.Directory;
import com.usatiuk.dhfs.storage.files.objects.File;
import com.usatiuk.dhfs.storage.files.service.DhfsFileService;
import io.quarkus.logging.Log;
import io.quarkus.runtime.Shutdown;
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

import java.nio.file.Paths;
import java.util.Optional;

import static jnr.posix.FileStat.S_IFDIR;
import static jnr.posix.FileStat.S_IFREG;

@ApplicationScoped
public class DhfsFuse extends FuseStubFS {
    @ConfigProperty(name = "dhfs.fuse.root")
    String root;

    @Inject
    DhfsFileService fileService;

    void init(@Observes @Priority(100000) StartupEvent event) {
        Paths.get(root).toFile().mkdirs();
        Log.info("Mounting with root " + root);

        var uid = new UnixSystem().getUid();
        var gid = new UnixSystem().getGid();

        mount(Paths.get(root), false, true,
                new String[]{"-o", "direct_io", "-o", "uid=" + String.valueOf(uid), "-o", "gid=" + String.valueOf(gid)});
    }

    @Override
    public int statfs(String path, Statvfs stbuf) {
        //FIXME:
        if ("/".equals(path)) {
            stbuf.f_blocks.set(1024 * 1024); // total data blocks in file system
            stbuf.f_frsize.set(1024);        // fs block size
            stbuf.f_bfree.set(1024 * 1024);  // free blocks in fs
            stbuf.f_bavail.set(1024 * 1024); // avail blocks in fs
        }
        return super.statfs(path, stbuf);
    }

    @Override
    public int getattr(String path, FileStat stat) {
        Optional<DirEntry> found;
        try {
            found = fileService.getDirEntry(path).await().indefinitely();
        } catch (Exception e) {
            Log.error("When accessing " + path, e);
            return -ErrorCodes.ENOENT();
        }
        if (found.isEmpty()) {
            return -ErrorCodes.ENOENT();
        }
        if (found.get() instanceof File f) {
            stat.st_mode.set(S_IFREG | f.getMode());
            stat.st_nlink.set(1);
            stat.st_size.set(fileService.size(f).await().indefinitely());
        } else if (found.get() instanceof Directory d) {
            stat.st_mode.set(S_IFDIR | d.getMode());
            stat.st_nlink.set(2);
        }
        return 0;
    }

    @Override
    public int open(String path, FuseFileInfo fi) {
        if (fileService.open(path).await().indefinitely().isEmpty()) return -ErrorCodes.ENOENT();
        return 0;
    }

    @Override
    public int read(String path, Pointer buf, long size, long offset, FuseFileInfo fi) {
        var fileOpt = fileService.open(path).await().indefinitely();
        if (fileOpt.isEmpty()) return -ErrorCodes.ENOENT();
        var file = fileOpt.get();
        var read = fileService.read(file.getUuid().toString(), offset, (int) size).await().indefinitely();
        if (read.isEmpty()) return 0;
        buf.put(0, read.get(), 0, read.get().length);
        return read.get().length;
    }

    @Override
    public int write(String path, Pointer buf, long size, long offset, FuseFileInfo fi) {
        var fileOpt = fileService.open(path).await().indefinitely();
        if (fileOpt.isEmpty()) return -ErrorCodes.ENOENT();
        var file = fileOpt.get();
        byte[] buffer = new byte[(int) size];
        buf.get(0, buffer, 0, (int) size);
        var written = fileService.write(file.getUuid().toString(), offset, buffer).await().indefinitely();
        return written.intValue();
    }

    @Override
    public int create(String path, long mode, FuseFileInfo fi) {
        var ret = fileService.create(path, mode).await().indefinitely();
        if (ret.isEmpty()) return -ErrorCodes.ENOSPC();
        else return 0;
    }

    @Override
    public int mkdir(String path, long mode) {
        var ret = fileService.mkdir(path, mode).await().indefinitely();
        if (ret.isEmpty()) return -ErrorCodes.ENOSPC();
        else return 0;
    }

    @Override
    public int rmdir(String path) {
        var ret = fileService.rmdir(path).await().indefinitely();
        if (!ret) return -ErrorCodes.ENOENT();
        else return 0;
    }

    @Override
    public int rename(String path, String newName) {
        var ret = fileService.rename(path, newName).await().indefinitely();
        if (!ret) return -ErrorCodes.ENOENT();
        else return 0;
    }

    @Override
    public int unlink(String path) {
        var ret = fileService.unlink(path).await().indefinitely();
        if (!ret) return -ErrorCodes.ENOENT();
        else return 0;
    }

    @Override
    public int truncate(String path, long size) {
        var fileOpt = fileService.open(path).await().indefinitely();
        if (fileOpt.isEmpty()) return -ErrorCodes.ENOENT();
        var file = fileOpt.get();
        var ok = fileService.truncate(file.getUuid().toString(), size).await().indefinitely();
        if (ok)
            return 0;
        else
            return -ErrorCodes.ENOSPC();
    }

    @Override
    public int chmod(String path, long mode) {
        var ret = fileService.chmod(path, mode).await().indefinitely();
        if (ret) return 0;
        else return -ErrorCodes.EINVAL();
    }

    @Override
    public int readdir(String path, Pointer buf, FuseFillDir filler, long offset, FuseFileInfo fi) {
        Iterable<String> found;
        try {
            found = fileService.readDir(path).await().indefinitely();
        } catch (Exception e) {
            return -ErrorCodes.ENOENT();
        }

        filler.apply(buf, ".", null, 0);
        filler.apply(buf, "..", null, 0);

        for (var c : found) {
            filler.apply(buf, c, null, 0);
        }

        return 0;
    }

    @Shutdown
    void shutdown() {
        Log.info("Unmounting");
        umount();
        Log.info("Unmounted");
    }
}
