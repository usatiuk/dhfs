package com.usatiuk.dhfsfs.objects;

import com.usatiuk.dhfs.jkleppmanntree.structs.JKleppmannTreeNodeMeta;
import com.usatiuk.objects.JObjectKey;

import java.util.Collection;
import java.util.List;

/**
 * JKleppmannTreeNodeMetaFile is a record that represents a file in the JKleppmann tree.
 * @param name the name of the file
 * @param fileIno a reference to the `File` object
 */
public record JKleppmannTreeNodeMetaFile(String name, JObjectKey fileIno) implements JKleppmannTreeNodeMeta {
    @Override
    public JKleppmannTreeNodeMeta withName(String name) {
        return new JKleppmannTreeNodeMetaFile(name, fileIno);
    }

    @Override
    public Collection<JObjectKey> collectRefsTo() {
        return List.of(fileIno);
    }
}
