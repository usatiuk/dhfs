package com.usatiuk.dhfs.objects.jkleppmanntree.structs;

import com.usatiuk.kleppmanntree.TreeNode;
import com.usatiuk.objects.common.runtime.JData;
import com.usatiuk.objects.common.runtime.JObjectKey;

import java.io.Serializable;
import java.util.UUID;

// FIXME: Ideally this is two classes?
public interface JKleppmannTreeNode extends JData, Serializable {
    TreeNode<Long, UUID, JKleppmannTreeNodeMeta, JObjectKey> getNode();

    void setNode(TreeNode<Long, UUID, JKleppmannTreeNodeMeta, JObjectKey> node);
}
