package server.sql;

import Zql.*;
import server.net.handler.frontend.FrontendConnection;
import server.net.response.OkResponse;
import engine.*;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.util.*;

public class SqlParser {
    static boolean explain = false;

    public static Predicate.Op getOp(String s) throws ParsingException {
        if (s.equals("="))
            return Predicate.Op.EQUALS;
        if (s.equals(">"))
            return Predicate.Op.GREATER_THAN;
        if (s.equals(">="))
            return Predicate.Op.GREATER_THAN_OR_EQ;
        if (s.equals("<"))
            return Predicate.Op.LESS_THAN;
        if (s.equals("<="))
            return Predicate.Op.LESS_THAN_OR_EQ;
        if (s.equals("LIKE"))
            return Predicate.Op.LIKE;
        if (s.equals("~"))
            return Predicate.Op.LIKE;
        if (s.equals("<>"))
            return Predicate.Op.NOT_EQUALS;
        if (s.equals("!="))
            return Predicate.Op.NOT_EQUALS;

        throw new ParsingException("Unknown predicate " + s);
    }

    void processExpression(TransactionId tid, ZExpression wx, LogicalPlan lp)
            throws ParsingException {
        if (wx.getOperator().equals("AND")) {
            for (int i = 0; i < wx.nbOperands(); i++) {
                if (!(wx.getOperand(i) instanceof ZExpression)) {
                    throw new ParsingException(
                            "Nested queries are currently unsupported.");
                }
                ZExpression newWx = (ZExpression) wx.getOperand(i);
                processExpression(tid, newWx, lp);

            }
        } else if (wx.getOperator().equals("OR")) {
            throw new ParsingException(
                    "OR expressions currently unsupported.");
        } else {
            // this is a binary expression comparing two constants
            @SuppressWarnings("unchecked")
            Vector<ZExp> ops = wx.getOperands();
            if (ops.size() != 2) {
                throw new ParsingException(
                        "Only simple binary expresssions of the form A op B are currently supported.");
            }

            boolean isJoin = false;
            Predicate.Op op = getOp(wx.getOperator());

            boolean op1const = ops.elementAt(0) instanceof ZConstant; // otherwise
                                                                      // is a
                                                                      // Query
            boolean op2const = ops.elementAt(1) instanceof ZConstant; // otherwise
                                                                      // is a
                                                                      // Query
            if (op1const && op2const) {
                isJoin = ((ZConstant) ops.elementAt(0)).getType() == ZConstant.COLUMNNAME
                        && ((ZConstant) ops.elementAt(1)).getType() == ZConstant.COLUMNNAME;
            } else if (ops.elementAt(0) instanceof ZQuery
                    || ops.elementAt(1) instanceof ZQuery) {
                isJoin = true;
            } else if (ops.elementAt(0) instanceof ZExpression
                    || ops.elementAt(1) instanceof ZExpression) {
                throw new ParsingException(
                        "Only simple binary expresssions of the form A op B are currently supported, where A or B are fields, constants, or subqueries.");
            } else
                isJoin = false;

            if (isJoin) { // join node

                String tab1field = "", tab2field = "";

                if (!op1const) { // left op is a nested query
                    // generate a virtual table for the left op
                    // this isn't a valid ZQL query
                } else {
                    tab1field = ((ZConstant) ops.elementAt(0)).getValue();

                }

                if (!op2const) { // right op is a nested query
                    try {
                        LogicalPlan sublp = parseQueryLogicalPlan(tid,
                                (ZQuery) ops.elementAt(1));
                        DbIterator pp = sublp.physicalPlan(tid,
                                TableStats.getStatsMap(), explain);
                        lp.addJoin(tab1field, pp, op);
                    } catch (IOException e) {
                        throw new ParsingException("Invalid subquery "
                                + ops.elementAt(1));
                    } catch (ParseException e) {
                        throw new ParsingException("Invalid subquery "
                                + ops.elementAt(1));
                    }
                } else {
                    tab2field = ((ZConstant) ops.elementAt(1)).getValue();
                    lp.addJoin(tab1field, tab2field, op);
                }

            } else { // select node
                String column;
                String compValue;
                ZConstant op1 = (ZConstant) ops.elementAt(0);
                ZConstant op2 = (ZConstant) ops.elementAt(1);
                if (op1.getType() == ZConstant.COLUMNNAME) {
                    column = op1.getValue();
                    compValue = new String(op2.getValue());
                } else {
                    column = op2.getValue();
                    compValue = new String(op1.getValue());
                }

                lp.addFilter(column, op, compValue);

            }
        }

    }

    public LogicalPlan parseQueryLogicalPlan(TransactionId tid, ZQuery q)
            throws IOException, ParseException, ParsingException {
        @SuppressWarnings("unchecked")
        Vector<ZFromItem> from = q.getFrom();
        LogicalPlan lp = new LogicalPlan();
        lp.setQuery(q.toString());
        // walk through tables in the FROM clause
        for (int i = 0; i < from.size(); i++) {
            ZFromItem fromIt = from.elementAt(i);
            try {

                int id = Database.getCatalog().getTableId(fromIt.getTable()); // will
                                                                              // fall
                                                                              // through
                                                                              // if
                                                                              // table
                                                                              // doesn't
                                                                              // exist
                String name;

                if (fromIt.getAlias() != null)
                    name = fromIt.getAlias();
                else
                    name = fromIt.getTable();

                lp.addScan(id, name);

                // XXX handle subquery?
            } catch (NoSuchElementException e) {
                e.printStackTrace();
                throw new ParsingException("Table "
                        + fromIt.getTable() + " is not in catalog");
            }
        }

        // now parse the where clause, creating Filter and Join nodes as needed
        ZExp w = q.getWhere();
        if (w != null) {

            if (!(w instanceof ZExpression)) {
                throw new ParsingException(
                        "Nested queries are currently unsupported.");
            }
            ZExpression wx = (ZExpression) w;
            processExpression(tid, wx, lp);

        }

        // now look for group by fields
        ZGroupBy gby = q.getGroupBy();
        String groupByField = null;
        if (gby != null) {
            @SuppressWarnings("unchecked")
            Vector<ZExp> gbs = gby.getGroupBy();
            if (gbs.size() > 1) {
                throw new ParsingException(
                        "At most one grouping field expression supported.");
            }
            if (gbs.size() == 1) {
                ZExp gbe = gbs.elementAt(0);
                if (!(gbe instanceof ZConstant)) {
                    throw new ParsingException(
                            "Complex grouping expressions (" + gbe
                                    + ") not supported.");
                }
                groupByField = ((ZConstant) gbe).getValue();
                System.out.println("GROUP BY FIELD : " + groupByField);
            }

        }

        // walk the select list, pick out aggregates, and check for query
        // validity
        @SuppressWarnings("unchecked")
        Vector<ZSelectItem> selectList = q.getSelect();
        String aggField = null;
        String aggFun = null;

        for (int i = 0; i < selectList.size(); i++) {
            ZSelectItem si = selectList.elementAt(i);
            if (si.getAggregate() == null
                    && (si.isExpression() && !(si.getExpression() instanceof ZConstant))) {
                throw new ParsingException(
                        "Expressions in SELECT list are not supported.");
            }
            if (si.getAggregate() != null) {
                if (aggField != null) {
                    throw new ParsingException(
                            "Aggregates over multiple fields not supported.");
                }
                aggField = ((ZConstant) ((ZExpression) si.getExpression())
                        .getOperand(0)).getValue();
                aggFun = si.getAggregate();
                System.out.println("Aggregate field is " + aggField
                        + ", agg fun is : " + aggFun);
                lp.addProjectField(aggField, aggFun);
            } else {
                if (groupByField != null
                        && !(groupByField.equals(si.getTable() + "."
                                + si.getColumn()) || groupByField.equals(si
                                .getColumn()))) {
                    throw new ParsingException("Non-aggregate field "
                            + si.getColumn()
                            + " does not appear in GROUP BY list.");
                }
                lp.addProjectField(si.getTable() + "." + si.getColumn(), null);
            }
        }

        if (groupByField != null && aggFun == null) {
            throw new ParsingException("GROUP BY without aggregation.");
        }

        if (aggFun != null) {
            lp.addAggregate(aggFun, aggField, groupByField);
        }
        // sort the data

        if (q.getOrderBy() != null) {
            @SuppressWarnings("unchecked")
            Vector<ZOrderBy> obys = q.getOrderBy();
            if (obys.size() > 1) {
                throw new ParsingException(
                        "Multi-attribute ORDER BY is not supported.");
            }
            ZOrderBy oby = obys.elementAt(0);
            if (!(oby.getExpression() instanceof ZConstant)) {
                throw new ParsingException(
                        "Complex ORDER BY's are not supported");
            }
            ZConstant f = (ZConstant) oby.getExpression();

            lp.addOrderBy(f.getValue(), oby.getAscOrder());

        }
        return lp;
    }

    private Transaction curtrans = null;
    private boolean inUserTrans = false;

    public Query handleQueryStatement(ZQuery s, TransactionId tId)
            throws TransactionAbortedException, DbException, IOException,
            ParsingException, ParseException {
        // and run it
        Query query = new Query(tId);

        LogicalPlan lp = parseQueryLogicalPlan(tId, s);
        DbIterator physicalPlan = lp.physicalPlan(tId,
                TableStats.getStatsMap(), explain);
        query.setPhysicalPlan(physicalPlan);
        query.setLogicalPlan(lp);

        if (physicalPlan != null) {
            Class<?> c;
            try {
                c = Class.forName("engine.OperatorCardinality");

                Class<?> p = Operator.class;
                Class<?> h = Map.class;

                java.lang.reflect.Method m = c.getMethod(
                        "updateOperatorCardinality", p, h, h);

                System.out.println("The query plan is:");
                m.invoke(null, (Operator) physicalPlan,
                        lp.getTableAliasToIdMapping(), TableStats.getStatsMap());
                c = Class.forName("engine.QueryPlanVisualizer");
                m = c.getMethod(
                        "printQueryPlanTree", DbIterator.class, System.out.getClass());
                m.invoke(c.newInstance(), physicalPlan,System.out);
            } catch (ClassNotFoundException e) {
            } catch (SecurityException e) {
            } catch (NoSuchMethodException e) {
                e.printStackTrace();
            } catch (IllegalArgumentException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            } catch (InstantiationException e) {
                e.printStackTrace();
            }
        }

        return query;
    }

    public Query handleInsertStatement(ZInsert s, TransactionId tId)
            throws TransactionAbortedException, DbException, IOException,
            ParsingException, ParseException {
        int tableId;
        try {
            tableId = Database.getCatalog().getTableId(s.getTable()); // will
                                                                      // fall
            // through if
            // table
            // doesn't
            // exist
        } catch (NoSuchElementException e) {
            throw new ParsingException("Unknown table : "
                    + s.getTable());
        }

        TupleDesc td = Database.getCatalog().getTupleDesc(tableId);

        Tuple t = new Tuple(td);
        int i = 0;
        DbIterator newTups;

        if (s.getValues() != null) {
            @SuppressWarnings("unchecked")
            Vector<ZExp> values = (Vector<ZExp>) s.getValues();
            if (td.numFields() != values.size()) {
                throw new ParsingException(
                        "INSERT statement does not contain same number of fields as table "
                                + s.getTable());
            }
            for (ZExp e : values) {

                if (!(e instanceof ZConstant))
                    throw new ParsingException(
                            "Complex expressions not allowed in INSERT statements.");
                ZConstant zc = (ZConstant) e;
                if (zc.getType() == ZConstant.NUMBER) {
                    if (td.getFieldType(i) != Type.INT_TYPE) {
                        throw new ParsingException("Value "
                                + zc.getValue()
                                + " is not an integer, expected a string.");
                    }
                    IntField f = new IntField(new Integer(zc.getValue()));
                    t.setField(i, f);
                } else if (zc.getType() == ZConstant.STRING) {
                    if (td.getFieldType(i) != Type.STRING_TYPE) {
                        throw new ParsingException("Value "
                                + zc.getValue()
                                + " is a string, expected an integer.");
                    }
                    StringField f = new StringField(zc.getValue(),
                            Type.STRING_LEN);
                    t.setField(i, f);
                } else {
                    throw new ParsingException(
                            "Only string or int fields are supported.");
                }

                i++;
            }
            ArrayList<Tuple> tups = new ArrayList<Tuple>();
            tups.add(t);
            newTups = new TupleArrayIterator(tups);

        } else {
            // insert into select
            ZQuery zq = (ZQuery) s.getQuery();
            LogicalPlan lp = parseQueryLogicalPlan(tId, zq);
            newTups = lp.physicalPlan(tId, TableStats.getStatsMap(), explain);
        }
        Query insertQ = new Query(tId);
        insertQ.setPhysicalPlan(new Insert(tId, newTups, tableId));
        return insertQ;
    }

    public Query handleDeleteStatement(ZDelete s, TransactionId tid)
            throws TransactionAbortedException, DbException, IOException,
            ParsingException, ParseException {
        int id;
        try {
            id = Database.getCatalog().getTableId(s.getTable()); // will fall
                                                                 // through if
                                                                 // table
                                                                 // doesn't
                                                                 // exist
        } catch (NoSuchElementException e) {
            throw new ParsingException("Unknown table : "
                    + s.getTable());
        }
        String name = s.getTable();
        Query sdbq = new Query(tid);

        LogicalPlan lp = new LogicalPlan();
        lp.setQuery(s.toString());

        lp.addScan(id, name);
        if (s.getWhere() != null)
            processExpression(tid, (ZExpression) s.getWhere(), lp);
        lp.addProjectField("null.*", null);

        DbIterator op = new Delete(tid, lp.physicalPlan(tid,
                TableStats.getStatsMap(), false));
        sdbq.setPhysicalPlan(op);

        return sdbq;

    }

    public void handleTransactStatement(ZTransactStmt s)
            throws TransactionAbortedException, DbException, IOException,
            ParsingException, ParseException {
        if (s.getStmtType().equals("COMMIT")) {
            if (curtrans == null)
                throw new ParsingException(
                        "No transaction is currently running");
            curtrans.commit();
            inUserTrans = false;
            System.out.println("Transaction " + curtrans.getId().getId() + " committed.");
            curtrans = null;
        } else if (s.getStmtType().equals("ROLLBACK")) {
            if (curtrans == null)
                throw new ParsingException("No transaction is currently running");
            curtrans.abort();
            inUserTrans = false;
            System.out.println("Transaction " + curtrans.getId().getId() + " aborted.");
            curtrans = null;
        } else if (s.getStmtType().equals("SET TRANSACTION")) {
            if (curtrans != null)
                throw new ParsingException("Can't start new transactions until current transaction has been committed or rolledback.");
            curtrans = new Transaction();
            curtrans.start();
            inUserTrans = true;
            System.out.println("Started a new transaction tid = " + curtrans.getId().getId());
        } else {
            throw new ParsingException("Unsupported operation");
        }
    }

    public LogicalPlan generateLogicalPlan(TransactionId tid, String s)
            throws ParsingException {
        ByteArrayInputStream bis = new ByteArrayInputStream(s.getBytes());
        ZqlParser p = new ZqlParser(bis);
        try {
            ZStatement stmt = p.readStatement();
            if (stmt instanceof ZQuery) {
                LogicalPlan lp = parseQueryLogicalPlan(tid, (ZQuery) stmt);
                return lp;
            }
        } catch (ParseException e) {
            throw new ParsingException(
                    "Invalid SQL expression: \n \t " + e);
        } catch (IOException e) {
            throw new ParsingException(e);
        }

        throw new ParsingException(
                "Cannot generate logical plan for expression : " + s);
    }

    public void setTransaction(Transaction t) {
        curtrans = t;
    }

    public Transaction getTransaction() {
        return curtrans;
    }

    public void processNextStatement(String s, FrontendConnection connection) {
        try {
            processNextStatement(new ByteArrayInputStream(s.getBytes("UTF-8")), connection);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public void processNextStatement(InputStream is, FrontendConnection connection) {
        try {
            ZqlParser p = new ZqlParser(is);
            ZStatement s = p.readStatement();

            Query query = null;
            if (s instanceof ZTransactStmt) {
                handleTransactStatement((ZTransactStmt) s);
                OkResponse.response(connection);
            } else {
                if (!this.inUserTrans) {
                    curtrans = new Transaction();
                    curtrans.start();
                    System.out.println("Started a new transaction tid = "
                            + curtrans.getId().getId());
                }
                try {
                    if (s instanceof ZInsert)
                        query = handleInsertStatement((ZInsert) s,
                                curtrans.getId());
                    else if (s instanceof ZDelete)
                        query = handleDeleteStatement((ZDelete) s,
                                curtrans.getId());
                    else if (s instanceof ZQuery)
                        query = handleQueryStatement((ZQuery) s,
                                curtrans.getId());
                    else {
                        System.out
                                .println("Can't parse "
                                        + s
                                        + "\n -- parser only handles SQL transactions, insert, delete, and select statements");
                    }
                    if (query != null)
                        query.execute(new WriteResultBefore(connection), new WriteResult(connection), new WriteResultAfter(connection));

                    if (!inUserTrans && curtrans != null) {
                        curtrans.commit();
                        System.out.println("Transaction "
                                + curtrans.getId().getId() + " committed.");
                    }
                } catch (Throwable a) {
                    // Whenever error happens, abort the current transaction
                    if (curtrans != null) {
                        curtrans.abort();
                        System.out.println("Transaction "
                                + curtrans.getId().getId()
                                + " aborted because of unhandled error");
                    }
                    this.inUserTrans = false;

                    if (a instanceof ParsingException
                            || a instanceof ParseException)
                        throw new ParsingException((Exception) a);
                    if (a instanceof TokenMgrError)
                        throw (TokenMgrError) a;
                    throw new DbException(a.getMessage());
                } finally {
                    if (!inUserTrans)
                        curtrans = null;
                }
            }

        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        } catch (DbException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParsingException e) {
            System.out
                    .println("Invalid SQL expression: \n \t" + e.getMessage());
        } catch (ParseException e) {
            System.out.println("Invalid SQL expression: \n \t " + e);
        } catch (TokenMgrError e) {
            System.out.println("Invalid SQL expression: \n \t " + e);
        }
    }

class TupleArrayIterator implements DbIterator {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    ArrayList<Tuple> tups;
    Iterator<Tuple> it = null;

    public TupleArrayIterator(ArrayList<Tuple> tups) {
        this.tups = tups;
    }

    public void open() throws DbException, TransactionAbortedException {
        it = tups.iterator();
    }

    /**
     * @return true if the iterator has more items.
     */
    public boolean hasNext() throws DbException, TransactionAbortedException {
        return it.hasNext();
    }

    /**
     * Gets the next tuple from the operator (typically implementing by reading
     * from a child operator or an access method).
     *
     * @return The next tuple in the iterator, or null if there are no more
     * tuples.
     */
    public Tuple next() throws DbException, TransactionAbortedException,
            NoSuchElementException {
        return it.next();
    }

    /**
     * Resets the iterator to the start.
     *
     * @throws DbException When rewind is unsupported.
     */
    public void rewind() throws DbException, TransactionAbortedException {
        it = tups.iterator();
    }

    /**
     * Returns the TupleDesc associated with this DbIterator.
     */
    public TupleDesc getTupleDesc() {
        return tups.get(0).getTupleDesc();
    }

    /**
     * Closes the iterator.
     */
    public void close() {
    }
}
}
