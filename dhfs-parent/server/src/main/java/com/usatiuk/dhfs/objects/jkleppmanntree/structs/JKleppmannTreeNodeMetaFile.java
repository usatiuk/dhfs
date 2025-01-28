package com.usatiuk.dhfs.objects.jkleppmanntree.structs;

import com.usatiuk.autoprotomap.runtime.ProtoMirror;
import com.usatiuk.dhfs.objects.JObjectKey;
import com.usatiuk.dhfs.objects.persistence.JKleppmannTreeNodeMetaFileP;

import java.util.Objects;

//@ProtoMirror(JKleppmannTreeNodeMetaFileP.class)
public class JKleppmannTreeNodeMetaFile extends JKleppmannTreeNodeMeta {
    private final JObjectKey _fileIno;

    public JKleppmannTreeNodeMetaFile(String name, JObjectKey fileIno) {
        super(name);
        _fileIno = fileIno;
    }

    public JObjectKey getFileIno() {
        return _fileIno;
    }

    @Override
    public JKleppmannTreeNodeMeta withName(String name) {
        return new JKleppmannTreeNodeMetaFile(name, _fileIno);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        JKleppmannTreeNodeMetaFile that = (JKleppmannTreeNodeMetaFile) o;
        return Objects.equals(_fileIno, that._fileIno);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), _fileIno);
    }
}
