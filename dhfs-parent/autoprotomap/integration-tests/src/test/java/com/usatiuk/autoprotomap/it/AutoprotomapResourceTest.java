package com.usatiuk.autoprotomap.it;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;

import com.usatiuk.autoprotomap.runtime.ProtoSerializer;
import io.quarkus.logging.Log;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;


@QuarkusTest
public class AutoprotomapResourceTest {

    @Inject
    ProtoSerializer<SimpleObjectProto, SimpleObject> protoSerializer;

    @Test
    public void testHelloEndpoint() {

        given()
                .when().get("/autoprotomap")
                .then()
                .statusCode(200)
                .body(is("Hello autoprotomap"));


        var ret = protoSerializer.serialize(new SimpleObject());
    }
}
