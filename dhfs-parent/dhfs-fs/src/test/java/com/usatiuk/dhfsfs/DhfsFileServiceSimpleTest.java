package com.usatiuk.dhfsfs;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

@QuarkusTest
@TestProfile(Profiles.DhfsFileServiceSimpleTestProfile.class)
public class DhfsFileServiceSimpleTest extends DhfsFileServiceSimpleTestImpl {
}
