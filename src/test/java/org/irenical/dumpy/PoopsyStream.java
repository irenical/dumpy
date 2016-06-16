package org.irenical.dumpy;

import org.irenical.dumpy.api.IExtractor;

import java.util.ArrayList;
import java.util.List;

public class PoopsyStream implements IExtractor< Integer > {

    private static final int RESPONSE_LIMIT = 1000;

    private static final int TOTAL = 5000;


    public PoopsyStream() {

    }

    @Override
    public Response< Integer > get(String cursor) {
        if ( cursor == null ) {
            cursor = "0";
        }
        Integer cursorIndex = Integer.valueOf(cursor);

//        make it a little slower
        try {
            Thread.sleep( 1000 );
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

//        generate a list of entities
        List< Entity< Integer > > entities = new ArrayList<>(RESPONSE_LIMIT);
        for (int i = 0; i < RESPONSE_LIMIT; i++ ) {
            entities.add( new Poopsy( cursorIndex + i ) );
        }

//        setup next iteration
        int nextCursor = cursorIndex + RESPONSE_LIMIT;
        boolean hasNext = nextCursor < TOTAL;

        return createResponse( entities, String.valueOf( nextCursor ), hasNext );
    }

    @Override
    public Response< Integer > get(List<String> ids, String cursor) {
        throw new UnsupportedOperationException("TODO");
    }


    private Response< Integer > createResponse( List< ? extends Entity< Integer > > entities, String cursor, boolean hasNext ) {
        Response< Integer > response = new Response<>();
        response.setEntities( entities );
        response.setCursor( cursor );
        response.setHasNext( hasNext );
        return response;
    }

}
