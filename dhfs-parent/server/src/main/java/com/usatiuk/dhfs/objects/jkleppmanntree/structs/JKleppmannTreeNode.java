package com.usatiuk.dhfs.objects.jkleppmanntree.structs;

import com.usatiuk.dhfs.objects.JDataRefcounted;
import com.usatiuk.kleppmanntree.OpMove;
import com.usatiuk.kleppmanntree.TreeNode;
import com.usatiuk.dhfs.objects.JObjectKey;
import lombok.Builder;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

// FIXME: Ideally this is two classes?
@Builder(toBuilder = true)
public record JKleppmannTreeNode(JObjectKey key, Collection<JObjectKey> refsFrom, boolean frozen, JObjectKey parent,
                                 OpMove<Long, UUID, JKleppmannTreeNodeMeta, JObjectKey> lastEffectiveOp,
                                 JKleppmannTreeNodeMeta meta,
                                 Map<String, JObjectKey> children) implements TreeNode<Long, UUID, JKleppmannTreeNodeMeta, JObjectKey>, JDataRefcounted, Serializable {

    public JKleppmannTreeNode(JObjectKey id, JObjectKey parent, JKleppmannTreeNodeMeta meta) {
        this(id, Collections.emptyList(), false, parent, null, meta, Collections.emptyMap());
    }

    @Override
    public JKleppmannTreeNode withParent(JObjectKey parent) {
        return this.toBuilder().parent(parent).build();
    }

    @Override
    public JKleppmannTreeNode withLastEffectiveOp(OpMove<Long, UUID, JKleppmannTreeNodeMeta, JObjectKey> lastEffectiveOp) {
        return this.toBuilder().lastEffectiveOp(lastEffectiveOp).build();
    }

    @Override
    public JKleppmannTreeNode withMeta(JKleppmannTreeNodeMeta meta) {
        return this.toBuilder().meta(meta).build();
    }

    @Override
    public JKleppmannTreeNode withChildren(Map<String, JObjectKey> children) {
        return this.toBuilder().children(children).build();
    }

    @Override
    public JKleppmannTreeNode withRefsFrom(Collection<JObjectKey> refs) {
        return this.toBuilder().refsFrom(refs).build();
    }

    @Override
    public JKleppmannTreeNode withFrozen(boolean frozen) {
        return this.toBuilder().frozen(frozen).build();
    }

    @Override
    public Collection<JObjectKey> collectRefsTo() {
        return Stream.concat(children().values().stream(),
                switch (meta()) {
                    case JKleppmannTreeNodeMetaDirectory dir -> Stream.<JObjectKey>of();
                    case JKleppmannTreeNodeMetaFile file -> Stream.<JObjectKey>of(file.getFileIno());
                    default -> throw new IllegalStateException("Unexpected value: " + meta());
                }
        ).toList();
    }
}
