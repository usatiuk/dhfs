package com.usatiuk.dhfs.objects.jkleppmanntree.structs;

import com.usatiuk.dhfs.objects.JObjectKey;

import java.util.Objects;

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

    @Override
    public String toString() {
        return "JKleppmannTreeNodeMetaFile{" +
                "_name=" + getName() + ", " +
                "_fileIno=" + _fileIno +
                '}';
    }
}
