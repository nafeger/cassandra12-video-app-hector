package com.killrvideo;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;


public interface IStatementActor {

	/**
	 * This method will be handed a craeted 
	 * @param statement
	 */
	Statement act(Connection conn) throws SQLException;

}
