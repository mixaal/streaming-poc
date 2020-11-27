package com.oracle.cloudsql.streamingfacade;

import com.oracle.bmc.retrier.RetryConfiguration;
import com.oracle.bmc.waiter.FixedTimeDelayStrategy;
import com.oracle.bmc.waiter.MaxAttemptsTerminationStrategy;


public class DefaultRetryConfigurationFactory {
    public static RetryConfiguration get() {
        return RetryConfiguration.builder()
                .delayStrategy(new FixedTimeDelayStrategy(2_000))
                .terminationStrategy(new MaxAttemptsTerminationStrategy(10))
                .build();
    }
}
