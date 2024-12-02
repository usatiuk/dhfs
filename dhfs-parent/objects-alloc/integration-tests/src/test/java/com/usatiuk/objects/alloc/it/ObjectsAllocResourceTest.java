package com.usatiuk.objects.alloc.it;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.is;

import org.junit.jupiter.api.Test;

import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
public class ObjectsAllocResourceTest {

    @Test
    public void testHelloEndpoint() {
        given()
                .when().get("/objects-alloc")
                .then()
                .statusCode(200)
                .body(is("Hello objects-alloc"));
    }
}
