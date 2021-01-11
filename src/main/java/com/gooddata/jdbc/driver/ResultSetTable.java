package com.gooddata.jdbc.driver;

import com.gooddata.jdbc.util.AbstractResultSet;
import com.gooddata.sdk.model.executeafm.result.*;

import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.*;
import java.util.*;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * ResultSet table
 */
public class ResultSetTable extends AbstractResultSet implements ResultSet {

	private final static Logger LOGGER = Logger.getLogger(ResultSetTable.class.getName());

	public static final int HOLDABILITY = CLOSE_CURSORS_AT_COMMIT;
	public static final int FETCH_DIRECTION =  FETCH_FORWARD;
	public static final int CONCURRENCY = CONCUR_READ_ONLY;
	public static final int TYPE = TYPE_SCROLL_INSENSITIVE;

	private final ExecutionResult afmExecutionResult;
	private final List<DatabaseMetaData.CatalogEntry> columns;
	private final Statement statement;

	private int[] columnStatementPosition;

	private int currentIndex = -1;

	/**
	 * Constructor
	 * @param statement SQL statement
	 * @param result GD AFM execution result
	 * @param columns AFM columns
	 */
	public ResultSetTable(Statement statement, ExecutionResult result,
						  List<DatabaseMetaData.CatalogEntry> columns) {
		this.afmExecutionResult = result;
		this.columns = columns;
		this.statement = statement;
		this.computeColumnsStatementPositions(columns);
	}

	/** Computes column index to AFM header and data structures */
	private void computeColumnsStatementPositions(List<DatabaseMetaData.CatalogEntry> columns) {
		this.columnStatementPosition =  new int[columns.size()];
		int metricPosition = 0;
		int attributePosition = 0;
		for(int i=0; i<this.columnStatementPosition.length; i++) {
			if(columns.get(i).getType().equals("metric")) {
				this.columnStatementPosition[i] = metricPosition++;
			}
			else {
				this.columnStatementPosition[i] = attributePosition++;
			}
		}
	}

	@Override
	public java.sql.ResultSetMetaData getMetaData() throws SQLException {
		return new ResultSetTableMetaData(this.columns);
	}

	/**
	 * Get textual value
	 * @param columnIndex 1 based index
	 * @return textual value
	 * @throws SQLException in case of issues
	 */
	public String getTextValue(int columnIndex) throws SQLException {
		if(this.currentIndex < 0 || this.currentIndex >= this.afmExecutionResult.getData().size())
			throw new SQLException("Cursor is out of range.");
		int realIndex = columnIndex - 1;
		if( realIndex >= this.columns.size() )
			throw new SQLException("Column index too high.");

		DatabaseMetaData.CatalogEntry column = this.columns.get(realIndex);
		if(column.getType().equals("metric")) {
			Data data = this.afmExecutionResult.getData().get(this.currentIndex);
			if(data instanceof DataList) {
				DataList row = (DataList)data;
				return row.get(this.columnStatementPosition[realIndex]).textValue();
			}
			else if(data instanceof DataValue) {
				DataValue value = (DataValue)data;
				return value.textValue();
			} else {
				throw new SQLException(String.format("ResultSetTable.getTextValue invalid data instance '%s'",
						data.getClass().getName()));
			}
		}
		else {
			return this.afmExecutionResult.getHeaderItems().get(0).get(this.columnStatementPosition[realIndex])
					.get(this.currentIndex).getName();
		}
	}

	/**
	 * Finds 1-based column index by column name
	 * @param columnLabel column name
	 * @return the 1-based column index
	 * @throws SQLException if the column doesn't exist
	 */
	@Override
	public int findColumn(String columnLabel) throws SQLException {
		int index = this.columns.stream().map(c->c.getTitle()).collect(Collectors.toList()).indexOf(columnLabel);
		if(index >= 0)
			return index + 1;
		else
			throw new SQLException(String.format("Column '%s' doesn't exist.", columnLabel));
	}

	@Override
	public Statement getStatement() throws SQLException {
		return this.statement;
	}

	@Override
	public boolean isBeforeFirst() {
		return this.currentIndex == -1;
	}

	@Override
	public boolean isAfterLast() {
		return this.currentIndex >= this.afmExecutionResult.getData().size();
	}

	@Override
	public boolean isFirst()  {
		return this.currentIndex == 0;
	}

	@Override
	public boolean isLast() throws SQLException {
		return this.currentIndex == this.afmExecutionResult.getData().size() - 1;
	}

	@Override
	public void beforeFirst() {
		this.currentIndex = -1;
	}

	@Override
	public void afterLast() {
		this.currentIndex = this.afmExecutionResult.getData().size();
	}

	@Override
	public boolean first() {
		this.currentIndex = 0;
		return true;
	}

	@Override
	public boolean last() {
		this.currentIndex = this.afmExecutionResult.getData().size() - 1;
		return true;
	}

	@Override
	public int getRow() {
		return this.currentIndex + 1;
	}

	@Override
	public boolean absolute(int row) {
		if(row > 0 && row <= this.afmExecutionResult.getData().size()) {
			this.currentIndex = row - 1;
			return true;
		}
		return false;
	}

	@Override
	public boolean relative(int rowsIncrement) {
		return absolute(this.currentIndex + 1 + rowsIncrement);
	}


}
