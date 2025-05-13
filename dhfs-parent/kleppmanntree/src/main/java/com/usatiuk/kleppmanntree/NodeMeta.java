package com.usatiuk.kleppmanntree;

import java.io.Serializable;

/**
 * Represents metadata associated with a node in the Kleppmann tree.
 * This interface is used to define the metadata that can be associated with nodes in the tree.
 * Implementations of this interface should provide a name for the node and a method to create a copy of it with a new name.
 */
public interface NodeMeta extends Serializable {
    /**
     * Returns the name of the node.
     *
     * @return the name of the node
     */
    String name();

    /**
     * Creates a copy of the metadata with a new name.
     *
     * @param name the new name for the metadata
     * @return a new instance of NodeMeta with the specified name
     */
    NodeMeta withName(String name);
}
