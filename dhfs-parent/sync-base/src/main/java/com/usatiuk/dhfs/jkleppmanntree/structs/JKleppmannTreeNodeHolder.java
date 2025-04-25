package com.usatiuk.dhfs.jkleppmanntree.structs;

import com.usatiuk.dhfs.refcount.JDataRef;
import com.usatiuk.dhfs.refcount.JDataRefcounted;
import com.usatiuk.objects.JObjectKey;
import org.pcollections.PCollection;
import org.pcollections.TreePSet;

import java.io.Serializable;
import java.util.Collection;
import java.util.Objects;

// Separate refcounting from JKleppmannTreeNode
public record JKleppmannTreeNodeHolder(PCollection<JDataRef> refsFrom, boolean frozen,
                                       JKleppmannTreeNode node) implements JDataRefcounted, Serializable {

    public JKleppmannTreeNodeHolder(JKleppmannTreeNode node) {
        this(TreePSet.empty(), false, node);
    }

    public JKleppmannTreeNodeHolder withNode(JKleppmannTreeNode node) {
        Objects.requireNonNull(node, "node");
        return new JKleppmannTreeNodeHolder(refsFrom, frozen, node);
    }

    @Override
    public JKleppmannTreeNodeHolder withRefsFrom(PCollection<JDataRef> refs) {
        return new JKleppmannTreeNodeHolder(refs, frozen, node);
    }

    @Override
    public JKleppmannTreeNodeHolder withFrozen(boolean frozen) {
        return new JKleppmannTreeNodeHolder(refsFrom, frozen, node);
    }

    @Override
    public Collection<JObjectKey> collectRefsTo() {
        return node.collectRefsTo();
    }

    @Override
    public JObjectKey key() {
        return node.key();
    }

    @Override
    public int estimateSize() {
        return node.estimateSize();
    }
}
