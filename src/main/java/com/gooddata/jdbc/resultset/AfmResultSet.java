package com.gooddata.jdbc.resultset;

import com.gooddata.jdbc.catalog.CatalogEntry;
import com.gooddata.jdbc.metadata.AfmResultSetMetaData;
import com.gooddata.sdk.model.executeafm.Execution;
import com.gooddata.sdk.model.executeafm.ResultPage;
import com.gooddata.sdk.model.executeafm.afm.Afm;
import com.gooddata.sdk.model.executeafm.response.ExecutionResponse;
import com.gooddata.sdk.model.executeafm.result.*;
import com.gooddata.sdk.model.executeafm.resultspec.Dimension;
import com.gooddata.sdk.model.executeafm.resultspec.ResultSpec;
import com.gooddata.sdk.model.executeafm.resultspec.SortItem;
import com.gooddata.sdk.model.project.Project;
import com.gooddata.sdk.service.FutureResult;
import com.gooddata.sdk.service.executeafm.ExecuteAfmService;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 * ResultSet table wraps AFM execute call result in JDBC ResultSet object
 */
public class AfmResultSet extends AbstractResultSet implements ResultSet {

	private final static Logger LOGGER = Logger.getLogger(AfmResultSet.class.getName());

	// GD workspace
	private final Project workspace;
	// AFM Service
	private final ExecuteAfmService gdAfm;
	// AFM execution spec
	private final Afm afm;
	// Current result
	private ExecutionResult afmExecutionResult;
	// Results paging
	private Paging paging;
	// current row offset in current page
	private int pageOffset = 0;
	// AFM columns
	private final List<CatalogEntry> columns;
	// JDBC statement
	private final Statement statement;
	// SQL LIMIT
	private final int sqlLimit;
	// SQL OFFSET
	private final int sqlOffset;
	// Order BY elements
	private final List<SortItem> orderBys;

	// Mapping between the column positions in AFM and in SELECT
	private int[] columnStatementPosition;
	private int currentRowNum = -1;

	/**
	 * Constructor
	 * @param statement SQL statement
	 * @param workspace GD workspace
	 * @param afmService GD AFM execution service
	 * @param afm AFM execution definition
	 * @param columns AFM columns
	 * @param orderBys SQL ORDER BY
	 * @param sqlLimit SQL LIMIT number
	 * @param sqlOffset SQL OFFSET number
	 */
	public AfmResultSet(Statement statement, Project workspace, ExecuteAfmService afmService, Afm afm,
						List<CatalogEntry> columns, List<SortItem> orderBys, int sqlLimit, int sqlOffset) {
		this.workspace = workspace;
		this.gdAfm = afmService;
		this.afm = afm;
		this.columns = columns;
		this.statement = statement;
		this.sqlLimit = sqlLimit;
		this.sqlOffset = sqlOffset;
		this.orderBys = orderBys;
		this.computeColumnsStatementPositions(columns);
		this.setFetchSize(1000);
		this.fetchPage(0);
	}

	private void fetchPage(int rowOffset) {
		Execution e;
		if (this.orderBys != null && this.orderBys.size() > 0) {
			List<Dimension> dimensions = new ArrayList<>();
			dimensions.add(new Dimension(this.columns.stream()
					.filter(i->i.getType().equals("attribute"))
					.map(i->i.getDefaultDisplayForm().getUri()).collect(Collectors.toList())));
			dimensions.add(new Dimension("measureGroup"));
			e = new Execution(afm, new ResultSpec(dimensions,this.orderBys));
		}
		else {
			e = new Execution(afm);
		}
		ExecutionResponse rs = this.gdAfm.executeAfm(this.workspace, e);
		List<Integer> offsets = Arrays.asList(rowOffset, 0);
		List<Integer> limits = Arrays.asList(this.fetchSize, this.columns.size());
		ResultPage resultPage = new ResultPage(offsets, limits);
		FutureResult<ExecutionResult> fr = this.gdAfm.getResult(rs, resultPage);
		this.afmExecutionResult = fr.get();
		this.paging = this.afmExecutionResult.getPaging();
		this.pageOffset = rowOffset;
	}

	public int getRowCount() {
		return Math.min(this.paging.getTotal().get(0) - this.sqlOffset, this.sqlLimit);
	}

	public int getMaxFetchedRow() {
		return this.paging.getOffset().get(0) + this.paging.getCount().get(0);
	}

	private void ensurePageFetched(int rowIndex) {
		if(rowIndex < this.getRowCount() && (rowIndex >= this.getMaxFetchedRow()
				|| rowIndex < this.pageOffset) ) {
			fetchPage(rowIndex);
		}
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
		if(this.currentRowNum < 0 || this.currentRowNum >= this.getRowCount())
			throw new SQLException("Cursor is out of range.");
		int realIndex = columnIndex - 1;
		if( realIndex >= this.columns.size() )
			throw new SQLException("Column index too high.");

		int rowNumWithOffset = this.currentRowNum + this.sqlOffset;
		this.ensurePageFetched(rowNumWithOffset);
		// index within the current page
		int actualPageRowIndex = rowNumWithOffset - this.pageOffset;

		CatalogEntry column = this.columns.get(realIndex);
		if(column.getType().equals("metric")) {
			Data data = this.afmExecutionResult.getData().get(actualPageRowIndex);
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
					.get(actualPageRowIndex).getName();
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
		return this.currentRowNum < 0;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isAfterLast() {
		return this.currentRowNum >= this.getRowCount();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isFirst()  {
		return this.currentRowNum == 0;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isLast() {
		return this.currentRowNum == this.getRowCount() - 1;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getRow() {
		return this.currentRowNum + 1;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean absolute(int row) {
		if(row > 0 && row <= this.getRowCount()) {
			this.currentRowNum =  row - 1;
			return true;
		}
		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean relative(int rowsIncrement) {
		return absolute(this.currentRowNum + 1 + rowsIncrement);
	}

}
