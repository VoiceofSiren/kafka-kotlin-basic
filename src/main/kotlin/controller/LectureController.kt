package org.ktor_lecture.controller

import org.ktor_lecture.basic.OrderEventPublisher
import org.ktor_lecture.model.CreateOrderRequest
import org.ktor_lecture.model.OrderEvent
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController
import java.util.UUID

@RestController
@RequestMapping("/api/lecture")
class LectureController (
    private val orderEventPublisher: OrderEventPublisher,
) {

    @PostMapping
    fun createOrder(
        @RequestBody createOrderRequest: CreateOrderRequest
    ): ResponseEntity<String> {
        val orderEvent = OrderEvent(
            orderId = UUID.randomUUID().toString(),
            customerId = createOrderRequest.customerId,
            quantity = createOrderRequest.quantity,
            price = createOrderRequest.price,
        )
    }
}