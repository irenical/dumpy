package org.irenical.dumpy.impl.db;

import org.irenical.drowsy.datasource.DrowsyDataSource;
import org.irenical.drowsy.query.BaseQuery;
import org.irenical.drowsy.query.Query;
import org.irenical.drowsy.query.builder.sql.InsertBuilder;
import org.irenical.drowsy.query.builder.sql.SelectBuilder;
import org.irenical.drowsy.query.builder.sql.UpdateBuilder;
import org.irenical.dumpy.impl.model.PaginatedResponse;
import org.irenical.jindy.ConfigFactory;
import org.irenical.lifecycle.LifeCycle;

import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DumpyDB implements LifeCycle {

    private final DrowsyDataSource dataSource;

    public DumpyDB( ) {
        dataSource = new DrowsyDataSource( ConfigFactory.getConfig().filterPrefix("dumpy.jdbc") ) {
            @Override
            protected boolean isAutoCommit() {
                return true;
            }
        };
    }


    @Override
    public <ERROR extends Exception> void start() throws ERROR {
        dataSource.start();
    }

    @Override
    public <ERROR extends Exception> void stop() throws ERROR {
        dataSource.stop();
    }

    @Override
    public <ERROR extends Exception> boolean isRunning() throws ERROR {
        return dataSource.isRunning();
    }


    public String getCursor(String jobCode, String streamCode ) throws SQLException {
        Query query = SelectBuilder
                .select( "cursor" )
                .from( "dumpy_stream" )
                .where( "job_code" ).eq( jobCode )
                .and( "stream_code").eq( streamCode )
                .build();

        return new JdbcSelectOperation<>( query, (rs) -> rs != null && rs.next() ? rs.getString( 1 ) : null )
                .run( dataSource );
    }

    public boolean setCursor(String jobCode, String streamCode, String cursor ) throws SQLException {
        Query query = UpdateBuilder.table( "dumpy_stream" ).literal( " SET " )
                .setParam( "cursor", cursor )
                .where( "job_code" ).eq( jobCode )
                .and( "stream_code" ).eq( streamCode )
                .build();

        return new JdbcUpdateOperation( query ).run( dataSource );
    }

    public Integer getStreamId( String jobCode, String streamCode ) throws SQLException {
        Query query = SelectBuilder.select( "id" )
                .from( "dumpy_stream" )
                .where( "job_code" ).eq( jobCode )
                .and( "stream_code" ).eq( streamCode )
                .build();

        return new JdbcSelectOperation<>( query, (rs) -> rs != null && rs.next() ? rs.getInt( 1 ) : null)
                .run( dataSource );
    }

    public Integer newStream( String jobCode, String streamCode ) throws SQLException {
        Query query = InsertBuilder.into( "dumpy_stream" )
                .columns( "job_code", "stream_code", "cursor" )
                .values( jobCode, streamCode, null )
                .build();

        return new JdbcInsertOperation( query )
                .run( dataSource );
    }

    /**
     *  over complicated query but its faster than 1 for each entity
     */
    public Boolean upsertEntities(
            String jobCode, String streamCode, Object[] entityIds,
            ZonedDateTime lastErrorStamp, ZonedDateTime lastUpdatedStamp )
    throws SQLException {
        if ( entityIds == null || entityIds.length == 0 ) {
            return false;
        }

//        build query string
        String querySQL = "WITH "
                + "queryStreamId AS ( "
                + "     SELECT id FROM dumpy_stream WHERE job_code = ? AND stream_code = ? "
                + ") "
                + ", entities( entityId ) AS ( VALUES ";

        for ( int idx = 0; idx < entityIds.length; idx++ ) {
            querySQL += "(?)" + ( ( idx < entityIds.length - 1 ) ? "," : "" );
        }

        querySQL += ")"
                + ", upsert AS ( "
                + "     UPDATE dumpy_stream_entity "
                + "         SET last_error_stamp=?, last_updated_stamp=? "
                + "         WHERE stream_id = ( SELECT id FROM queryStreamId ) "
                + "             AND entity_id IN ( ";

        for ( int idx = 0; idx < entityIds.length; idx++ ) {
            querySQL += "?" + ( ( idx < entityIds.length - 1 ) ? "," : "" );
        }

        querySQL += "           ) RETURNING * "
                + ") "
                + "INSERT INTO dumpy_stream_entity ( stream_id, entity_id, last_error_stamp, last_updated_stamp ) "
                + "     SELECT queryStreamId.id, entities.entityId, ?, ? FROM queryStreamId, entities WHERE NOT EXISTS ( SELECT * FROM upsert )";

//        set query parameters
        List< Object > parameters = new ArrayList<>();
        parameters.add( jobCode );
        parameters.add( streamCode );
        Collections.addAll(parameters, entityIds);
        parameters.add( lastErrorStamp );
        parameters.add( lastUpdatedStamp );
        Collections.addAll(parameters, entityIds);
        parameters.add( lastErrorStamp );
        parameters.add( lastUpdatedStamp );

        BaseQuery query = new BaseQuery();
        query.setType(Query.TYPE.UPDATE );
        query.setQuery( querySQL );
        query.setParameters( parameters );

        return new JdbcUpdateOperation( query ).run( dataSource );
    }

    public PaginatedResponse< String > get(String jobCode, String streamCode, String cursor ) throws SQLException {
        Integer offset = cursor == null || cursor.trim().isEmpty() ? 0 : Integer.valueOf(cursor);

        Query query = SelectBuilder.select("dumpy_stream_entity.entity_id")
                .from("dumpy_stream_entity")
                .innerJoin("dumpy_stream")
                    .on("( dumpy_stream.id = dumpy_stream_entity.stream_id AND dumpy_stream.job_code").eq( jobCode )
                        .literal( " AND dumpy_stream.stream_code" ).eq( streamCode ).literal( ") " )
                .where("dumpy_stream_entity.last_error_stamp").notEq( (Object) null )
                .literal( " ORDER BY dumpy_stream_entity.id " )
                .literal( " OFFSET ").param(offset).literal( " LIMIT 10 " )
                .build();

        return new JdbcSelectOperation<>(query, rs -> {
            List< String > result = new ArrayList<>();
            while( rs.next() ) {
                result.add( rs.getString( 1 ) );
            }

            PaginatedResponse< String > cursorResponse = new PaginatedResponse<>();
            cursorResponse.values = result;
            cursorResponse.cursor = String.valueOf( offset + 10 );
            cursorResponse.hasNext = result.size() >= 10;

            return cursorResponse;
        }).run( dataSource );
    }

}
