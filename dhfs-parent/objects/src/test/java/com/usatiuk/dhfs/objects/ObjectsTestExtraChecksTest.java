package com.usatiuk.dhfs.objects;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

@QuarkusTest
@TestProfile(Profiles.ObjectsTestProfileExtraChecks.class)
public class ObjectsTestExtraChecksTest extends ObjectsTestImpl {
}
