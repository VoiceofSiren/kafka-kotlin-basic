package org.ktor_lecture.controller

import org.ktor_lecture.model.OrderCountComparisonStats
import org.ktor_lecture.model.StatsResponse
import org.ktor_lecture.stream.OrderStreamsService
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/api/stats")
class StatsController (
    private val orderStreamsService: OrderStreamsService,
) {

    @GetMapping("/orders/count")
    fun getOrderCountStats(): ResponseEntity<StatsResponse<OrderCountComparisonStats>> {
        val stats = orderStreamsService.compareOrderCount()
        val response = StatsResponse(
            success = true,
            data = stats,
            message = "Order count stats retrieved successfully",
        )
        return ResponseEntity.ok(response)
    }
}