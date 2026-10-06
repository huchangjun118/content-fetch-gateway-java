package com.github.huchangjun118.contentfetchgatewayjava.gateway;

import java.util.concurrent.CompletionStage;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Read-only HTTP facade over the gateway.
 */
@RestController
@RequestMapping("/gateway/content")
public class GatewayController {

    private final GatewayService service;

    public GatewayController(GatewayService service) {
        this.service = service;
    }

    @GetMapping(value = "/{id}", produces = MediaType.APPLICATION_OCTET_STREAM_VALUE)
    public CompletionStage<ResponseEntity<byte[]>> get(@PathVariable String id) {
        return service.get(new ContentId(id))
                .thenApply(body -> ResponseEntity.ok()
                        .contentType(MediaType.APPLICATION_OCTET_STREAM)
                        .body(body))
                .exceptionally(error -> ResponseEntity
                        .status(HttpStatus.BAD_GATEWAY)
                        .body(new byte[0]));
    }
}
