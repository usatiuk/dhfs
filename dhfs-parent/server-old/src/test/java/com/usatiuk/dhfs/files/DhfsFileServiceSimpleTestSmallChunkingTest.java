package com.usatiuk.dhfs.files;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

@QuarkusTest
@TestProfile(Profiles.DhfsFileServiceSimpleTestProfileSmallChunking.class)
public class DhfsFileServiceSimpleTestSmallChunkingTest extends DhfsFileServiceSimpleTestImpl {
}
