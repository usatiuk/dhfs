package com.usatiuk.dhfs.objects.jkleppmanntree.structs;

import com.usatiuk.dhfs.objects.*;
import com.usatiuk.dhfs.objects.repository.peersync.structs.JKleppmannTreeNodeMetaPeer;
import com.usatiuk.kleppmanntree.OpMove;
import com.usatiuk.kleppmanntree.TreeNode;
import org.pcollections.HashTreePMap;
import org.pcollections.PCollection;
import org.pcollections.PMap;
import org.pcollections.TreePSet;

import java.io.Serializable;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// FIXME: Ideally this is two classes?
public record JKleppmannTreeNode(JObjectKey key, PCollection<JDataRef> refsFrom, boolean frozen, JObjectKey parent,
                                 OpMove<Long, PeerId, JKleppmannTreeNodeMeta, JObjectKey> lastEffectiveOp,
                                 JKleppmannTreeNodeMeta meta,
                                 PMap<String, JObjectKey> children) implements TreeNode<Long, PeerId, JKleppmannTreeNodeMeta, JObjectKey>, JDataRefcounted, Serializable {

    public JKleppmannTreeNode(JObjectKey id, JObjectKey parent, JKleppmannTreeNodeMeta meta) {
        this(id, TreePSet.empty(), false, parent, null, meta, HashTreePMap.empty());
    }

    @Override
    public JKleppmannTreeNode withParent(JObjectKey parent) {
        return new JKleppmannTreeNode(key, refsFrom, frozen, parent, lastEffectiveOp, meta, children);
    }

    @Override
    public JKleppmannTreeNode withLastEffectiveOp(OpMove<Long, PeerId, JKleppmannTreeNodeMeta, JObjectKey> lastEffectiveOp) {
        return new JKleppmannTreeNode(key, refsFrom, frozen, parent, lastEffectiveOp, meta, children);
    }

    @Override
    public JKleppmannTreeNode withMeta(JKleppmannTreeNodeMeta meta) {
        return new JKleppmannTreeNode(key, refsFrom, frozen, parent, lastEffectiveOp, meta, children);
    }

    @Override
    public JKleppmannTreeNode withChildren(PMap<String, JObjectKey> children) {
        return new JKleppmannTreeNode(key, refsFrom, frozen, parent, lastEffectiveOp, meta, children);
    }

    @Override
    public JKleppmannTreeNode withRefsFrom(PCollection<JDataRef> refs) {
        return new JKleppmannTreeNode(key, refs, frozen, parent, lastEffectiveOp, meta, children);
    }

    @Override
    public JKleppmannTreeNode withFrozen(boolean frozen) {
        return new JKleppmannTreeNode(key, refsFrom, frozen, parent, lastEffectiveOp, meta, children);
    }

    @Override
    public Collection<JDataRef> collectRefsTo() {
        return Stream.<JDataRef>concat(children().values().stream().map(JDataNormalRef::new),
                switch (meta()) {
                    case JKleppmannTreeNodeMetaDirectory dir -> Stream.<JDataNormalRef>of();
                    case JKleppmannTreeNodeMetaFile file -> Stream.of(new JDataNormalRef(file.getFileIno()));
                    case JKleppmannTreeNodeMetaPeer peer -> Stream.of(new JDataNormalRef(peer.getPeerId()));
                    default -> throw new IllegalStateException("Unexpected value: " + meta());
                }
        ).collect(Collectors.toUnmodifiableSet());
    }
}
