package com.usatiuk.dhfs.objects.jklepmanntree.structs;

import lombok.Getter;

public class JTreeNodeMetaFile extends JTreeNodeMeta {
    @Getter
    private final String _fileIno;

    public JTreeNodeMetaFile(String name, String fileIno) {
        super(name);
        _fileIno = fileIno;
    }

    @Override
   public JTreeNodeMeta withName(String name) {
        return new JTreeNodeMetaFile(name, _fileIno);
    }
}
