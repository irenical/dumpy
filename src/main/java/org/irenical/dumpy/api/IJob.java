package org.irenical.dumpy.api;

import java.util.List;

public interface IJob {

    String getCode();

    List< IStream > getStreams();

}
