package com.usatiuk.dhfs.objects;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;

@QuarkusTest
@TestProfile(Profiles.ObjectsTestProfileNoExtraChecks.class)
public class ObjectsTestNoExtraChecks extends ObjectsTestImpl {
}
