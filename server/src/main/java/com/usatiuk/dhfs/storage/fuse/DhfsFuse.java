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
import java.time.Duration;
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
            return ErrorCodes.ENOENT();
        }
        if (found.isEmpty()) return ErrorCodes.ENOENT();
        if (found.get() instanceof File) {
            stat.st_mode.set(S_IFREG | 0777);
            stat.st_nlink.set(1);
            stat.st_size.set(0);
        } else if (found.get() instanceof Directory) {
            stat.st_mode.set(S_IFDIR | 0777);
            stat.st_nlink.set(2);
        }
        return 0;
    }

    @Override
    public int open(String path, FuseFileInfo fi) {
        if (fileService.open(path).await().indefinitely().isEmpty()) return ErrorCodes.ENOENT();
        return 0;
    }

    @Override
    public int read(String path, Pointer buf, long size, long offset, FuseFileInfo fi) {
        return super.read(path, buf, size, offset, fi);
    }

    @Override
    public int write(String path, Pointer buf, long size, long offset, FuseFileInfo fi) {
        return super.write(path, buf, size, offset, fi);
    }

    @Override
    public int readdir(String path, Pointer buf, FuseFillDir filler, long offset, FuseFileInfo fi) {
        Iterable<String> found;
        try {
            found = fileService.readDir(path).await().indefinitely();
        } catch (Exception e) {
            return ErrorCodes.ENOENT();
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
