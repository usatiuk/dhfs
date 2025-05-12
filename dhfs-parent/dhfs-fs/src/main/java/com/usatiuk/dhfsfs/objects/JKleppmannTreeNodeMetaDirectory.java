package com.usatiuk.dhfsfs.objects;

import com.usatiuk.dhfs.jkleppmanntree.structs.JKleppmannTreeNodeMeta;
import com.usatiuk.objects.JObjectKey;

import java.util.Collection;
import java.util.List;

/**
 * JKleppmannTreeNodeMetaDirectory is a record that represents a directory in the JKleppmann tree.
 * @param name the name of the directory
 */
public record JKleppmannTreeNodeMetaDirectory(String name) implements JKleppmannTreeNodeMeta {
    public JKleppmannTreeNodeMeta withName(String name) {
        return new JKleppmannTreeNodeMetaDirectory(name);
    }

    @Override
    public Collection<JObjectKey> collectRefsTo() {
        return List.of();
    }
}
