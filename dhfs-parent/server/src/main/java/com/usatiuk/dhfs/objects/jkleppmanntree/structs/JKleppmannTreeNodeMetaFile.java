package com.usatiuk.dhfs.objects.jkleppmanntree.structs;

import com.usatiuk.autoprotomap.runtime.ProtoMirror;
import com.usatiuk.dhfs.objects.persistence.JKleppmannTreeNodeMetaFileP;
import lombok.Getter;

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
}
