package com.usatiuk.dhfs.objects.jkleppmanntree.structs;

import com.usatiuk.dhfs.objects.JDataRefcounted;
import com.usatiuk.kleppmanntree.TreeNode;
import com.usatiuk.objects.common.runtime.JObjectKey;

import java.io.Serializable;
import java.util.Collection;
import java.util.UUID;
import java.util.stream.Stream;

// FIXME: Ideally this is two classes?
public interface JKleppmannTreeNode extends JDataRefcounted, Serializable {
    TreeNode<Long, UUID, JKleppmannTreeNodeMeta, JObjectKey> getNode();

    void setNode(TreeNode<Long, UUID, JKleppmannTreeNodeMeta, JObjectKey> node);

    @Override
    default Collection<JObjectKey> collectRefsTo() {
        return Stream.concat(getNode().getChildren().values().stream(),
                switch (getNode().getMeta()) {
                    case JKleppmannTreeNodeMetaDirectory dir -> Stream.<JObjectKey>of();
                    case JKleppmannTreeNodeMetaFile file -> Stream.<JObjectKey>of(file.getFileIno());
                    default -> throw new IllegalStateException("Unexpected value: " + getNode().getMeta());
                }
        ).toList();
    }
}
