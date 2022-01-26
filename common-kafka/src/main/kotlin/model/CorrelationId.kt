package model

import java.util.*

class CorrelationId private constructor() {

    lateinit var id: String

    constructor(title: String) : this() {
        this.id = "$title(${UUID.randomUUID()})"
    }

    fun continueWith(title: String): CorrelationId {
        return CorrelationId("$id-$title")
    }

    override fun toString(): String {
        return "CorrelationId(id='$id')"
    }

}
