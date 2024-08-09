package com.usatiuk.dhfs.objects.jkleppmanntree.structs;

import lombok.Getter;

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
