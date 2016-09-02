package org.irenical.dumpy.impl.model;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;

public class DumpyBlockingQueue extends LinkedBlockingQueue<Runnable> {

    private static final long serialVersionUID = -2308346772990442986L;

    private static final Logger LOGGER = LoggerFactory.getLogger( DumpyBlockingQueue.class );

    public DumpyBlockingQueue(int capacity) {
        super(capacity);
    }

    @Override
    public boolean offer(Runnable runnable) {
        try {
            put(runnable);
            return true;
        } catch (InterruptedException e) {
            LOGGER.error( e.getLocalizedMessage(), e );
            Thread.currentThread().interrupt();
        }
        return false;
    }

}