package org.irenical.dumpy.api;

import org.irenical.lifecycle.LifeCycle;

public interface IStreamProcessor extends LifeCycle {

    < TYPE, ERROR extends Exception > void process( IJob iJob, IStream< TYPE, ERROR > iStream ) throws Exception;

}
