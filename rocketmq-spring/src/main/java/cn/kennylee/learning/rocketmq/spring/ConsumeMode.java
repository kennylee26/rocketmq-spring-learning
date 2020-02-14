package cn.kennylee.learning.rocketmq.spring;

/**
 * <p> Control consume mode, you can choice receive message concurrently or orderly. </p>
 * <p>Created on 14/2/2020.</p>
 *
 * @author kennylee
 */
public enum ConsumeMode {
    /**
     * 并发访问
     */
    CONCURRENTLY,
    /**
     * 顺序访问
     */
    ORDERLY
}
