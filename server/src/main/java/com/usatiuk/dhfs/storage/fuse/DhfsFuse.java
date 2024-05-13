package com.usatiuk.dhfs.storage.fuse;

import io.quarkus.logging.Log;
import io.quarkus.runtime.Shutdown;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jnr.ffi.Pointer;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;

import java.nio.file.Paths;

@ApplicationScoped
public class DhfsFuse extends FuseStubFS {
    @ConfigProperty(name = "dhfs.fuse.root")
    String root;

    void init(@Observes @Priority(100000) StartupEvent event) {
        Paths.get(root).toFile().mkdirs();
        Log.info("Mounting with root " + root);

        mount(Paths.get(root));
    }

    @Override
    public int getattr(String path, FileStat stat) {
        return super.getattr(path, stat);
    }

    @Override
    public int open(String path, FuseFileInfo fi) {
        return super.open(path, fi);
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
    public int readdir(String path, Pointer buf, FuseFillDir filter, long offset, FuseFileInfo fi) {
        return super.readdir(path, buf, filter, offset, fi);
    }

    @Shutdown
    void shutdown() {
        Log.info("Unmounting");
        umount();
        Log.info("Unmounted");
    }
}
