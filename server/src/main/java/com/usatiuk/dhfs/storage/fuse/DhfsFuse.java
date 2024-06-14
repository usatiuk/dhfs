package com.usatiuk.dhfs.storage.fuse;

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

        mount(Paths.get(root));
    }

    @Override
    public int getattr(String path, FileStat stat) {
        Optional<DirEntry> found;
        try {
            found = fileService.getDirEntry(path).await().indefinitely();
        } catch (Exception e) {
            Log.error(e);
            return -ErrorCodes.ENOENT();
        }
        if (found.isEmpty()) {
            return -ErrorCodes.ENOENT();
        }
        if (found.get() instanceof File f) {
            stat.st_mode.set(S_IFREG | 0777);
            stat.st_nlink.set(1);
            stat.st_size.set(fileService.size(f).await().indefinitely());
        } else if (found.get() instanceof Directory) {
            stat.st_mode.set(S_IFDIR | 0777);
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
        var ret = fileService.create(path).await().indefinitely();
        if (ret.isEmpty()) return -ErrorCodes.ENOSPC();
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
