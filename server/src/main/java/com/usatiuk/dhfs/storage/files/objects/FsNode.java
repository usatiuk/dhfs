package com.usatiuk.dhfs.storage.files.objects;

import com.usatiuk.dhfs.storage.objects.jrepository.JObjectData;
import com.usatiuk.dhfs.storage.objects.repository.distributed.ConflictResolver;
import lombok.Getter;
import lombok.Setter;

import java.util.UUID;

public abstract class FsNode extends JObjectData {
    @Getter
    final UUID _uuid;

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

    @Getter
    @Setter
    private long _mode;

    @Getter
    @Setter
    private long _ctime;

    @Getter
    @Setter
    private long _mtime;
}
