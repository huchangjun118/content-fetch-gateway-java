package com.github.huchangjun118.contentfetchgatewayjava.gateway;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 对上层提供稳定一致的内容读取入口。
 * 返回的内容一定通过完整性校验；所有来源不可用或超时返回 502。
 */
@RestController
@RequestMapping("/gateway/content")
public class FetchController {

    private final FetchGateway fetchGateway;

    public FetchController(FetchGateway fetchGateway) {
        this.fetchGateway = fetchGateway;
    }

    @GetMapping("/{cid}")
    public ResponseEntity<byte[]> get(@PathVariable String cid) {
        try {
            FetchResult result = fetchGateway.fetch(cid);
            return ResponseEntity.ok()
                    .contentType(MediaType.APPLICATION_OCTET_STREAM)
                    .header("X-Content-Source", result.sourceId())
                    .header("X-Content-From-Cache", String.valueOf(result.fromCache()))
                    .body(result.content());
        } catch (FetchException e) {
            return ResponseEntity.status(HttpStatus.BAD_GATEWAY).build();
        }
    }
}
