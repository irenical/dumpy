package org.irenical.dumpy.impl.db;

import org.irenical.drowsy.datasource.DrowsyDataSource;
import org.irenical.drowsy.query.Query;
import org.irenical.drowsy.query.SQLQueryBuilder;
import org.irenical.dumpy.impl.model.PaginatedResponse;
import org.irenical.jindy.ConfigFactory;
import org.irenical.lifecycle.LifeCycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

public class DumpyDB implements LifeCycle {

    private static final Logger LOGGER = LoggerFactory.getLogger( DumpyDB.class );

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
        Query query = SQLQueryBuilder
                .select("SELECT cursor FROM dumpy_stream WHERE job_code").eq( jobCode )
                    .append(" AND stream_code").eq( streamCode )
                .build();

        return new JdbcSelectOperation<>( query, (rs) -> rs != null && rs.next() ? rs.getString( 1 ) : null )
                .run( dataSource );
    }

    public boolean setCursor(String jobCode, String streamCode, String cursor ) throws SQLException {
        Query query = SQLQueryBuilder
                .update("UPDATE dumpy_stream SET cursor=").param( cursor )
                .append( " WHERE job_code").eq( jobCode )
                .append( " AND stream_code").eq( streamCode )
                .build();

        return new JdbcUpdateOperation( query ).run( dataSource );
    }

    public Integer getStreamId( String jobCode, String streamCode ) throws SQLException {
        Query query = SQLQueryBuilder
                .select("SELECT id FROM dumpy_stream WHERE job_code").eq( jobCode )
                .append(" AND stream_code" ).eq( streamCode )
                .build();

        return new JdbcSelectOperation<>( query, (rs) -> rs != null && rs.next() ? rs.getInt( 1 ) : null)
                .run( dataSource );
    }

    public Integer newStream( String jobCode, String streamCode ) throws SQLException {
        Query query = SQLQueryBuilder
                .insert("INSERT INTO dumpy_stream (job_code, stream_code, cursor) VALUES (")
                .param(jobCode).append(",").param(streamCode).append(",").param(null)
                .append(")")
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

        SQLQueryBuilder queryBuilder = SQLQueryBuilder.update("WITH ")
                .append("queryStreamId AS ( ")
                .append("  SELECT id FROM dumpy_stream WHERE job_code").eq(jobCode).append(" AND stream_code").eq(streamCode)
                .append(")");

        queryBuilder.append( ", entities( entityId ) AS ( VALUES ");
        int idx = 0;
        for (Object entityId : entityIds) {
            queryBuilder.append( "(").param( entityId ).append( ")");
            if ( idx++ < entityIds.length - 1 ) {
                queryBuilder.append( ", ");
            }
        }
        queryBuilder.append( " ) ");

        queryBuilder
                .append(", upsert AS ( ")
                .append("    UPDATE dumpy_stream_entity SET last_error_stamp=").param(lastErrorStamp).append(", last_updated_stamp=").param(lastUpdatedStamp)
                .append("    WHERE stream_id = ( SELECT id FROM queryStreamId ) AND entity_id").in( entityIds )
                .append("    RETURNING * ")
                .append(") ")
                .append("INSERT INTO dumpy_stream_entity ( stream_id, entity_id, last_error_stamp, last_updated_stamp ) ")
                .append("    SELECT queryStreamId.id, entities.entityId, ").param(lastErrorStamp).append(", ").param(lastUpdatedStamp)
                .append("   FROM queryStreamId, entities")
                .append("    WHERE NOT EXISTS ( SELECT * FROM upsert )");

        Query query = queryBuilder.build();

        return new JdbcUpdateOperation( query ).run( dataSource );
    }




    public PaginatedResponse< String > get(String jobCode, String streamCode, String cursor ) throws SQLException {
        Integer offset = cursor == null || cursor.trim().isEmpty() ? 0 : Integer.valueOf(cursor);

        Query query = SQLQueryBuilder.select("SELECT dumpy_stream_entity.entity_id " +
                "FROM dumpy_stream_entity " +
                "  INNER JOIN dumpy_stream ON ( dumpy_stream.id = dumpy_stream_entity.stream_id " +
                "                               AND dumpy_stream.job_code=").param(jobCode)
                .append("                       AND dumpy_stream.stream_code=").param(streamCode)
                .append(" ) " +
                        "WHERE " +
                        "  dumpy_stream_entity.last_error_stamp IS NOT NULL " +
                        "ORDER BY dumpy_stream_entity.id " +
                        "OFFSET ").param( offset ).append(" LIMIT 10" )
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
