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
        testNode1._tree.move(testNode1._storageInterface.getRootId(), new TestNodeMetaDir("Test1"), d1id);
        testNode2._tree.move(testNode1._storageInterface.getRootId(), new TestNodeMetaDir("Test2"), d2id);

        {
            var r1 = testNode1.getRecorded();
            Assertions.assertEquals(1, r1.size());
            testNode2._tree.applyExternalOp(1L, r1.getFirst());

            var r2 = testNode2.getRecorded();
            Assertions.assertEquals(1, r2.size());
            testNode1._tree.applyExternalOp(2L, r2.getFirst());
        }

        Assertions.assertEquals(d1id, testNode1._tree.traverse(List.of("Test1")));
        Assertions.assertEquals(d2id, testNode1._tree.traverse(List.of("Test2")));
        Assertions.assertEquals(d1id, testNode2._tree.traverse(List.of("Test1")));
        Assertions.assertEquals(d2id, testNode2._tree.traverse(List.of("Test2")));

        Assertions.assertIterableEquals(List.of("Test1", "Test2"), testNode1._storageInterface.getById(testNode2._storageInterface.getRootId()).getNode().getChildren().keySet());
        Assertions.assertIterableEquals(List.of("Test1", "Test2"), testNode2._storageInterface.getById(testNode2._storageInterface.getRootId()).getNode().getChildren().keySet());

        var f1id = testNode1._storageInterface.getNewNodeId();

        testNode1._tree.move(d2id, new TestNodeMetaFile("TestFile", 1234), f1id);
        {
            var r1 = testNode1.getRecorded();
            Assertions.assertEquals(1, r1.size());
            testNode2._tree.applyExternalOp(1L, r1.getFirst());
        }

        Assertions.assertEquals(f1id, testNode1._tree.traverse(List.of("Test2", "TestFile")));
        Assertions.assertEquals(f1id, testNode2._tree.traverse(List.of("Test2", "TestFile")));

        var cop1 = new OpMove<>(new CombinedTimestamp<>(testNode1._clock.getTimestamp(), 1L),
                                d1id,
                                new TestNodeMetaDir("Test2"),
                                d2id);
        testNode1._tree.move(d1id, new TestNodeMetaDir("Test2"), d2id);
        Assertions.assertEquals(d1id, testNode1._tree.traverse(List.of("Test1")));
        Assertions.assertEquals(d2id, testNode1._tree.traverse(List.of("Test1", "Test2")));
        Assertions.assertIterableEquals(List.of("Test1"), testNode1._storageInterface.getById(testNode2._storageInterface.getRootId()).getNode().getChildren().keySet());

        testNode2._tree.move(d2id,new TestNodeMetaDir("Test1"),d1id);
        Assertions.assertIterableEquals(List.of("Test2"), testNode2._storageInterface.getById(testNode2._storageInterface.getRootId()).getNode().getChildren().keySet());
        Assertions.assertEquals(d2id, testNode2._tree.traverse(List.of("Test2")));
        Assertions.assertEquals(d1id, testNode2._tree.traverse(List.of("Test2", "Test1")));

        {
            var r1 = testNode1.getRecorded();
            Assertions.assertEquals(1, r1.size());
            testNode2._tree.applyExternalOp(1L, r1.getFirst());

            var r2 = testNode2.getRecorded();
            Assertions.assertEquals(1, r2.size());
            testNode1._tree.applyExternalOp(2L, r2.getFirst());
        }

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
