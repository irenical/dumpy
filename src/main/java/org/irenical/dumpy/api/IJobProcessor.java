package org.irenical.dumpy.api;

import org.irenical.lifecycle.LifeCycle;

import java.util.function.Consumer;

public interface IJobProcessor extends LifeCycle {

    < ERROR extends Exception > void accept( IJob iJob ) throws ERROR;

}
