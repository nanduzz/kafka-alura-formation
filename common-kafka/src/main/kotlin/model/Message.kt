package model

data class Message<T>(
    val id: CorrelationId,
    val payload: T
)

