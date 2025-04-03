package com.usatiuk.dhfs.jkleppmanntree.structs;

import com.usatiuk.dhfs.JDataRef;
import com.usatiuk.dhfs.JDataRefcounted;
import com.usatiuk.objects.JObjectKey;
import com.usatiuk.dhfs.PeerId;
import com.usatiuk.dhfs.repository.peersync.structs.JKleppmannTreeNodeMetaPeer;
import com.usatiuk.kleppmanntree.OpMove;
import com.usatiuk.kleppmanntree.TreeNode;
import com.usatiuk.objects.JObjectKeyImpl;
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
    public Collection<JObjectKey> collectRefsTo() {
        return Stream.<JObjectKey>concat(children().values().stream(),
                switch (meta()) {
                    case JKleppmannTreeNodeMetaDirectory dir -> Stream.<JObjectKey>empty();
                    case JKleppmannTreeNodeMetaFile file -> Stream.of(file.getFileIno());
                    case JKleppmannTreeNodeMetaPeer peer -> Stream.of(peer.getPeerId());
                    case null -> Stream.<JObjectKey>empty();
                    default -> throw new IllegalStateException("Unexpected value: " + meta());
                }
        ).collect(Collectors.toUnmodifiableSet());
    }
}
