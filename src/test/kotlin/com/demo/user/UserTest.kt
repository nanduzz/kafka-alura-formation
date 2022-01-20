package com.demo.user

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertAll
import java.util.*
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

internal class UserTest {

    @Test
    fun shouldCreateAnUser() {
        val u = User(
            id = UUID.randomUUID(),
            name = "teste",
            email = "teste@test.com"
        )
        assertAll(
            { assertEquals(1, 1) },
            { assertNotNull(u) }
        )

    }
}
