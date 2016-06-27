package org.irenical.dumpy.impl.db;

import org.irenical.drowsy.query.Query;
import org.irenical.drowsy.transaction.JdbcOperation;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class JdbcSelectOperation< OUTPUT > extends JdbcOperation< OUTPUT > {

    @FunctionalInterface
    public interface JdbcReaderFunction< OUTPUT > {

        OUTPUT apply(ResultSet resultSet) throws SQLException;

    }


    private final Query query;

    private final JdbcReaderFunction< OUTPUT > readerFunction;

    public JdbcSelectOperation( Query query, JdbcReaderFunction< OUTPUT > readerFunction ) {
        this.query = query;
        this.readerFunction = readerFunction;
    }

    @Override
    protected OUTPUT execute(Connection connection) throws SQLException {
        PreparedStatement statement = query.createPreparedStatement(connection);
        ResultSet rs = statement.executeQuery();
        return readerFunction.apply( rs );
    }

}
