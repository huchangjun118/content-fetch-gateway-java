package com.github.huchangjun118.contentfetchgatewayjava.gateway;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 来源可用性评估：持续失败或返回损坏内容的来源会被熔断一段时间，
 * 冷却结束后允许探测流量进入，探测成功则恢复调度。
 *
 * <p>状态机：CLOSED（正常）→ 连续失败达到阈值 → OPEN（退出调度）
 * → 冷却期满 → HALF_OPEN（放行一个探测请求）→ 探测成功回 CLOSED，失败回 OPEN。</p>
 */
public class SourceHealthTracker {

    public enum State {
        CLOSED, OPEN, HALF_OPEN
    }

    public enum FailureKind {
        /** 来源抖动 / 网络错误。 */
        ERROR,
        /** 返回了未通过完整性校验的内容，权重更高。 */
        CORRUPTION
    }

    private static final class Health {
        State state = State.CLOSED;
        int consecutiveFailures;
        long openedAt;
        boolean probeInFlight;
    }

    private final int failureThreshold;
    private final long openCooldownMillis;
    private final Map<String, Health> healthMap = new ConcurrentHashMap<>();

    public SourceHealthTracker(int failureThreshold, long openCooldownMillis) {
        this.failureThreshold = Math.max(1, failureThreshold);
        this.openCooldownMillis = Math.max(0, openCooldownMillis);
    }

    /**
     * 当前是否允许该来源参与调度。
     * OPEN 且冷却期满时转为 HALF_OPEN 并放行一个探测请求；
     * 探测进行期间其余请求不再放行该来源。
     */
    public boolean allow(String sourceId) {
        Health h = healthMap.computeIfAbsent(sourceId, k -> new Health());
        synchronized (h) {
            switch (h.state) {
                case CLOSED:
                    return true;
                case OPEN:
                    if (System.currentTimeMillis() - h.openedAt >= openCooldownMillis) {
                        h.state = State.HALF_OPEN;
                        h.probeInFlight = true;
                        return true;
                    }
                    return false;
                case HALF_OPEN:
                    if (h.probeInFlight) {
                        return false;
                    }
                    h.probeInFlight = true;
                    return true;
                default:
                    return false;
            }
        }
    }

    /** 记录一次成功：来源恢复正常，重新参与调度。 */
    public void recordSuccess(String sourceId) {
        Health h = healthMap.computeIfAbsent(sourceId, k -> new Health());
        synchronized (h) {
            h.state = State.CLOSED;
            h.consecutiveFailures = 0;
            h.probeInFlight = false;
        }
    }

    /** 记录一次失败；达到阈值后熔断。损坏内容按两次失败计。 */
    public void recordFailure(String sourceId, FailureKind kind) {
        Health h = healthMap.computeIfAbsent(sourceId, k -> new Health());
        synchronized (h) {
            h.consecutiveFailures += kind == FailureKind.CORRUPTION ? 2 : 1;
            h.probeInFlight = false;
            if (h.state == State.HALF_OPEN || h.consecutiveFailures >= failureThreshold) {
                h.state = State.OPEN;
                h.openedAt = System.currentTimeMillis();
            }
        }
    }

    public State state(String sourceId) {
        Health h = healthMap.computeIfAbsent(sourceId, k -> new Health());
        synchronized (h) {
            if (h.state == State.OPEN
                    && System.currentTimeMillis() - h.openedAt >= openCooldownMillis) {
                return State.HALF_OPEN;
            }
            return h.state;
        }
    }
}
