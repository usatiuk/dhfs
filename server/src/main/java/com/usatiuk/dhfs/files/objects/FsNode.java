package com.usatiuk.dhfs.files.objects;

import com.usatiuk.dhfs.files.conflicts.NotImplementedConflictResolver;
import com.usatiuk.dhfs.objects.jrepository.JObjectData;
import com.usatiuk.dhfs.objects.repository.ConflictResolver;
import lombok.Getter;
import lombok.Setter;

import java.io.Serial;
import java.util.UUID;

public abstract class FsNode extends JObjectData {
    @Serial
    private static final long serialVersionUID = 1;

    @Getter
    final UUID _uuid;
    @Getter
    @Setter
    private long _mode;
    @Getter
    @Setter
    private long _ctime;
    @Getter
    @Setter
    private long _mtime;

    protected FsNode(UUID uuid) {
        this._uuid = uuid;
        this._ctime = System.currentTimeMillis();
        this._mtime = this._ctime;
    }

    protected FsNode(UUID uuid, long mode) {
        this._uuid = uuid;
        this._mode = mode;
        this._ctime = System.currentTimeMillis();
        this._mtime = this._ctime;
    }

    @Override
    public String getName() {
        return _uuid.toString();
    }

    @Override
    public Class<? extends ConflictResolver> getConflictResolver() {
        return NotImplementedConflictResolver.class;
    }
}
