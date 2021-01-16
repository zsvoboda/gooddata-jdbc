package com.gooddata.jdbc.driver;

import com.gooddata.jdbc.util.AbstractResultSet;
import com.gooddata.sdk.model.executeafm.result.Data;
import com.gooddata.sdk.model.executeafm.result.DataList;
import com.gooddata.sdk.model.executeafm.result.DataValue;
import com.gooddata.sdk.model.executeafm.result.ExecutionResult;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * ResultSet table wraps AFM execute call result in JDBC ResultSet object
 */
public class AfmResultSet extends AbstractResultSet implements ResultSet {

	private final static Logger LOGGER = Logger.getLogger(AfmResultSet.class.getName());

	// AFM execution result
	private final ExecutionResult afmExecutionResult;
	// AFM columns
	private final List<CatalogEntry> columns;
	// JDBC statement
	private final Statement statement;

	// Mapping between the column positions in AFM and in SELECT
	private int[] columnStatementPosition;
	private int currentIndex = -1;

	/**
	 * Constructor
	 * @param statement SQL statement
	 * @param result GD AFM execution result
	 * @param columns AFM columns
	 */
	public AfmResultSet(Statement statement, ExecutionResult result,
						List<CatalogEntry> columns) {
		this.afmExecutionResult = result;
		this.columns = columns;
		this.statement = statement;
		this.computeColumnsStatementPositions(columns);
	}

	public int getRowCount() {
		return this.afmExecutionResult.getData().size();
	}

	/**
	 *  Computes column index to AFM header and data structures
	 * @param columns SQL columns
	 * */
	private void computeColumnsStatementPositions(List<CatalogEntry> columns) {
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

	/**
	 * {@inheritDoc}
	 */
	@Override
	public java.sql.ResultSetMetaData getMetaData() {
		return new AfmResultSetMetaData(this.columns);
	}

	/**
	 * Get textual value - extracts text value from AFM execution result
	 * @param columnIndex 1 based index
	 * @return textual value
	 * @throws SQLException in case of issues
	 */
	public String getTextValue(int columnIndex) throws SQLException {
		if(this.currentIndex < 0 || this.currentIndex >= this.getRowCount())
			throw new SQLException("Cursor is out of range.");
		int realIndex = columnIndex - 1;
		if( realIndex >= this.columns.size() )
			throw new SQLException("Column index too high.");

		CatalogEntry column = this.columns.get(realIndex);
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
		int index = this.columns.stream().map(CatalogEntry::getTitle).collect(Collectors.toList()).indexOf(columnLabel);
		if(index >= 0)
			return index + 1;
		else
			throw new SQLException(String.format("Column '%s' doesn't exist.", columnLabel));
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Statement getStatement() {
		return this.statement;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isBeforeFirst() {
		return this.currentIndex == -1;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isAfterLast() {
		return this.currentIndex >= this.getRowCount();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isFirst()  {
		return this.currentIndex == 0;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isLast() {
		return this.currentIndex == this.getRowCount() - 1;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void beforeFirst() {
		this.currentIndex = -1;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void afterLast() {
		this.currentIndex = this.getRowCount();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean first() {
		this.currentIndex = 0;
		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean last() {
		this.currentIndex = this.getRowCount() - 1;
		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getRow() {
		return this.currentIndex + 1;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean absolute(int row) {
		if(row > 0 && row <= this.getRowCount()) {
			this.currentIndex = row - 1;
			return true;
		}
		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean relative(int rowsIncrement) {
		return absolute(this.currentIndex + 1 + rowsIncrement);
	}

}
