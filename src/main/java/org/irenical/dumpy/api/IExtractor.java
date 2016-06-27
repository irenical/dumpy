package org.irenical.dumpy.api;

import org.irenical.dumpy.impl.model.PaginatedResponse;

import java.util.List;

public interface IExtractor< TYPE, ERROR extends Exception > {

    interface Entity< TYPE > {
        /**
         *
         * @return  Entity unique id in the source system.
         */
        String getId();

        /**
         *
         * @return  entity type
         */
        TYPE getValue();
    }

    class Response< TYPE > extends PaginatedResponse< Entity< TYPE > > {

    }


    /**
     * Get entities from the source system since the given cursor.
     * Cursor can be null (or empty) - usually on the first run.
     *
     * @param cursor
     * @return  a paginated response for the requested entities
     */
    Response< TYPE > get(String cursor) throws ERROR;

    /**
     * Get entities from the source system identified by the given ids.
     *
     * @param ids
     * @param cursor
     * @return  a paginated response for the requested entities
     */
    Response< TYPE > get(List< String > ids, String cursor) throws ERROR;


    /**
     * Utility to build a paginated response.
     */
    default Response< TYPE > response( List< Entity< TYPE > > entities, String nextCursor, boolean hasNext ) {
        Response< TYPE > response = new Response<>();
        response.setValues( entities );
        response.setCursor( nextCursor );
        response.setHasNext( hasNext );
        return response;
    }

}
