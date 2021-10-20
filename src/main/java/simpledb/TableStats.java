package simpledb;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing proj1 and proj2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    int tableid;
    int ioCostPerPage;
    private int numOfTuples;
    private HeapFile file;
    private Object[] histograms;
    private HashMap<String, Integer> minStats;
    private HashMap<String, Integer> maxStats;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        this.tableid= tableid;
        this.ioCostPerPage = ioCostPerPage;
        numOfTuples = 0;
        minStats = new HashMap<>();
        maxStats = new HashMap<>();
        DbFile table = Database.getCatalog().getDbFile(tableid);
        file = (HeapFile) table;
        histograms = new Object[table.getTupleDesc().numFields()];
        Transaction transaction= new Transaction();
        transaction.start();
        DbFileIterator iterator = table.iterator(transaction.getId());

        try {
            createStats(iterator);
            createHistograms(iterator);
            iterator.close();
        } catch (DbException e) {
            e.printStackTrace();
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        }

        try {
            transaction.commit();
        } catch(IOException e) {
            e.printStackTrace();
        }

    }

    private void createStats(DbFileIterator iterator) throws DbException, TransactionAbortedException {
        iterator.open();
        while (iterator.hasNext()) {
            numOfTuples++;
            Tuple tuple = iterator.next();
            TupleDesc tupleDesc = tuple.getTupleDesc();
            for(int i = 0; i< tupleDesc.numFields(); i++) {
                Field field = tuple.getField(i);
                if(Type.INT_TYPE.equals(field.getType())) {
                    IntField intField = (IntField) field;
                    int value = intField.getValue();
                    String fieldName = tupleDesc.getFieldName(i);
                    Integer min = minStats.get(fieldName);
                    if(min == null || value < min) {
                        minStats.put(fieldName, value);
                    }
                    Integer max = maxStats.get(fieldName);
                    if(max == null || value > max) {
                        maxStats.put(fieldName, value);
                    }
                }
            }
        }
    }

    private void createHistograms(DbFileIterator iterator) throws DbException, TransactionAbortedException {
        iterator.rewind();
        while (iterator.hasNext()) {
            Tuple tuple = iterator.next();
            TupleDesc tupleDesc = tuple.getTupleDesc();
            for (int i = 0; i < tupleDesc.numFields(); i++) {
                Field field = tuple.getField(i);
                String fieldName = tupleDesc.getFieldName(i);
                if (Type.INT_TYPE.equals(field.getType())) {
                    IntField intField = (IntField) field;
                    int value = intField.getValue();
                    if(histograms[i] == null) {
                        histograms[i] = new IntHistogram(NUM_HIST_BINS, minStats.get(fieldName), maxStats.get(fieldName));
                    }
                    IntHistogram intHistogram = (IntHistogram)histograms[i];
                    intHistogram.addValue(value);
                }
                if (Type.STRING_TYPE.equals(field.getType())) {
                    StringField stringField = (StringField) field;
                    String value = stringField.getValue();
                    if(histograms[i] == null) {
                        histograms[i] = new StringHistogram(NUM_HIST_BINS);
                    }
                    StringHistogram stringHistogram = (StringHistogram)histograms[i];
                    stringHistogram.addValue(value);
                }
            }
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return file.numPages() * ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int)(numOfTuples * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        //return 1.0;
        if (file.getTupleDesc().getFieldType(field).equals(Type.INT_TYPE)) {
            return ((IntHistogram) histograms[field]).avgSelectivity();
        }
        return ((StringHistogram)histograms[field]).avgSelectivity();
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        //return 1.0;
        if (constant.getType().equals(Type.INT_TYPE)) {
            int v = ((IntField)constant).getValue();
            return ((IntHistogram)histograms[field]).estimateSelectivity(op, v);
        }
        String str = ((StringField)constant).getValue();
        return ((StringHistogram)histograms[field]).estimateSelectivity(op, str);
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return numOfTuples;
    }

}
