package com.usatiuk.dhfsfs;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

@QuarkusTest
@TestProfile(Profiles.DhfsFileServiceSimpleTestProfileNoChunking.class)
public class DhfsFileServiceSimpleTestNoChunkingTest extends DhfsFileServiceSimpleTestImpl {
}
