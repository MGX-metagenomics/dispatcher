package de.cebitec.mgx.dispatcher;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author sjaenick
 * 
 */
public class ThreadPoolExecutor extends java.util.concurrent.ThreadPoolExecutor {

    protected static final int defaultCorePoolSize = 5;
    protected static final int defaultMaximumPoolSize = 10;
    protected static final long defaultKeepAliveTime = 10;
    protected static final TimeUnit defaultTimeUnit = TimeUnit.MINUTES;
    protected static final BlockingQueue<Runnable> queue = new LinkedBlockingQueue<>();
    protected static ThreadPoolExecutor instance;

    private ThreadPoolExecutor() {
        super(defaultCorePoolSize, defaultMaximumPoolSize, defaultKeepAliveTime, defaultTimeUnit, queue);
    }

    public synchronized static ThreadPoolExecutor getInstance() {
        if (instance == null) {
            instance = new ThreadPoolExecutor();
        }
        return instance;
    }
}
