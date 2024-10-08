package com.usatiuk.dhfs.objects.jkleppmanntree.structs;

import com.usatiuk.autoprotomap.runtime.ProtoMirror;
import com.usatiuk.dhfs.objects.persistence.JKleppmannTreeNodeMetaFileP;
import lombok.Getter;

import java.util.Objects;

@ProtoMirror(JKleppmannTreeNodeMetaFileP.class)
public class JKleppmannTreeNodeMetaFile extends JKleppmannTreeNodeMeta {
    @Getter
    private final String _fileIno;

    public JKleppmannTreeNodeMetaFile(String name, String fileIno) {
        super(name);
        _fileIno = fileIno;
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
