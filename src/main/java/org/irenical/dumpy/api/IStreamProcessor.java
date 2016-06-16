package org.irenical.dumpy.api;

import org.irenical.lifecycle.LifeCycle;

public interface IStreamProcessor extends LifeCycle {

    < TYPE > void process( IJob iJob, IStream< TYPE > iStream ) throws Exception;

}
