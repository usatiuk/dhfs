package com.usatiuk.kleppmanntree;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class KleppmanTreeSimpleTest {
    private final TestNode testNode1 = new TestNode(1);
    private final TestNode testNode2 = new TestNode(2);


    @Test
    void circularTest() {
        var d1id = testNode1._storageInterface.getNewNodeId();
        var d2id = testNode2._storageInterface.getNewNodeId();
        var op1 = new OpMove<>(new CombinedTimestamp<>(testNode1._clock.getTimestamp(), 1L),
                               testNode1._storageInterface.getRootId(),
                               new TestNodeMetaDir("Test1"),
                               d1id);
        var op2 = new OpMove<>(new CombinedTimestamp<>(testNode2._clock.getTimestamp(), 2L),
                               testNode2._storageInterface.getRootId(),
                               new TestNodeMetaDir("Test2"),
                               d2id);
        testNode1._tree.applyOp(1L, op1);
        testNode2._tree.applyOp(2L, op2);
        testNode1._tree.applyOp(2L, op2);
        testNode2._tree.applyOp(1L, op1);

        Assertions.assertEquals(d1id, testNode1._tree.traverse(List.of("Test1")));
        Assertions.assertEquals(d2id, testNode1._tree.traverse(List.of("Test2")));
        Assertions.assertEquals(d1id, testNode2._tree.traverse(List.of("Test1")));
        Assertions.assertEquals(d2id, testNode2._tree.traverse(List.of("Test2")));

        Assertions.assertIterableEquals(List.of("Test1", "Test2"), testNode1._storageInterface.getById(testNode2._storageInterface.getRootId()).getNode().getChildren().keySet());
        Assertions.assertIterableEquals(List.of("Test1", "Test2"), testNode2._storageInterface.getById(testNode2._storageInterface.getRootId()).getNode().getChildren().keySet());

        var f1id = testNode1._storageInterface.getNewNodeId();
        var op3 = new OpMove<>(new CombinedTimestamp<>(testNode1._clock.getTimestamp(), 1L),
                               d2id,
                               new TestNodeMetaFile("TestFile", 1234),
                               f1id);
        testNode1._tree.applyOp(1L, op3);
        testNode2._tree.applyOp(1L, op3);

        Assertions.assertEquals(f1id, testNode1._tree.traverse(List.of("Test2", "TestFile")));
        Assertions.assertEquals(f1id, testNode2._tree.traverse(List.of("Test2", "TestFile")));

        var cop1 = new OpMove<>(new CombinedTimestamp<>(testNode1._clock.getTimestamp(), 1L),
                                d1id,
                                new TestNodeMetaDir("Test2"),
                                d2id);
        testNode1._tree.applyOp(1L, cop1);
        Assertions.assertEquals(d1id, testNode1._tree.traverse(List.of("Test1")));
        Assertions.assertEquals(d2id, testNode1._tree.traverse(List.of("Test1", "Test2")));
        Assertions.assertIterableEquals(List.of("Test1"), testNode1._storageInterface.getById(testNode2._storageInterface.getRootId()).getNode().getChildren().keySet());

        var cop2 = new OpMove<>(new CombinedTimestamp<>(testNode2._clock.getTimestamp(), 2L),
                                d2id,
                                new TestNodeMetaDir("Test1"),
                                d1id);
        testNode2._tree.applyOp(2L, cop2);
        Assertions.assertIterableEquals(List.of("Test2"), testNode2._storageInterface.getById(testNode2._storageInterface.getRootId()).getNode().getChildren().keySet());
        Assertions.assertEquals(d2id, testNode2._tree.traverse(List.of("Test2")));
        Assertions.assertEquals(d1id, testNode2._tree.traverse(List.of("Test2", "Test1")));

        testNode1._tree.applyOp(2L, cop2);
        testNode2._tree.applyOp(1L, cop1);
        // Second node wins as it has smaller timestamp
        Assertions.assertIterableEquals(List.of("Test2"), testNode1._storageInterface.getById(testNode2._storageInterface.getRootId()).getNode().getChildren().keySet());
        Assertions.assertIterableEquals(List.of("Test1", "TestFile"), testNode1._storageInterface.getById(d2id).getNode().getChildren().keySet());
        Assertions.assertEquals(d2id, testNode1._tree.traverse(List.of("Test2")));
        Assertions.assertEquals(d1id, testNode1._tree.traverse(List.of("Test2", "Test1")));
        Assertions.assertEquals(f1id, testNode1._tree.traverse(List.of("Test2", "TestFile")));

        var f11 = testNode1._storageInterface.getById(f1id);
        var f12 = testNode2._storageInterface.getById(f1id);

        Assertions.assertEquals(f11.getNode().getMeta(), f12.getNode().getMeta());
        Assertions.assertInstanceOf(TestNodeMetaFile.class, f11.getNode().getMeta());

        // Trim test
        Assertions.assertTrue(testNode1._storageInterface.getLog().size() <= 1);
        Assertions.assertTrue(testNode2._storageInterface.getLog().size() <= 1);
    }

}
