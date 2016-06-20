package org.irenical.dumpy.api;

import org.irenical.dumpy.impl.model.PaginatedResponse;

import java.util.List;

public interface IExtractor< TYPE > {

    interface Entity< TYPE > {
        String getId();
        TYPE getValue();
    }


    class Response< TYPE > extends PaginatedResponse< Entity< TYPE > > {

    }


    Response< TYPE > get(String cursor);

    Response< TYPE > get(List< String > ids, String cursor);

}
