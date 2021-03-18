package com.tianyuan;

import simpledb.*;

import java.io.File;
import java.io.IOException;

/**
 * @Author: 刘天元
 * @Date: 2021/3/18 14:49
 */
public class Test {
    public static void main(String[] args) throws IOException {
        // construct a 3-column table schema
        Type[] types = new Type[]{ Type.INT_TYPE, Type.INT_TYPE, Type.INT_TYPE };
        String[] names = new String[]{ "field0", "field1", "field2" };

        TupleDesc td = new TupleDesc(types, names);

        // create the tables, associate them with the data files
        // and tell the catalog about the schema  the tables.
        File file1 = new File("F:/db/simpledb/some_data_file1.dat");
        HeapFile table1 = new HeapFile(file1, td);
        Database.getCatalog().addTable(table1, "t1");

        File file2 = new File("F:/db/simpledb/some_data_file2.dat");
        HeapFile table2 = new HeapFile(file2, td);
        Database.getCatalog().addTable(table2, "t2");

        // construct the query: we use two SeqScans, which spoonfeed
        // tuples via iterators into join
        TransactionId tid = new TransactionId();

        for (int i = 0; i < 504; ++i) {
            try {
                System.out.println(i);
                Database.getBufferPool().insertTuple(tid,table1.getId(), getHeapTuple(i, 3));
                Database.getBufferPool().insertTuple(tid, table2.getId(), getHeapTuple(i, 3));
            } catch (DbException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (TransactionAbortedException e) {
                e.printStackTrace();
            }
        }
        Database.getBufferPool().flushPages(tid);

        SeqScan ss1 = new SeqScan(tid, table1.getId(), "t1");
        SeqScan ss2 = new SeqScan(tid, table2.getId(), "t2");

        // create a filter for the where condition
        Filter sf1 = new Filter(
                new Predicate(0,
                        Predicate.Op.GREATER_THAN, new IntField(1)),  ss1);

        JoinPredicate p = new JoinPredicate(1, Predicate.Op.EQUALS, 1);
        Join j = new Join(p, sf1, ss2);

        // and run it
        try {
            j.open();
            while (j.hasNext()) {
                Tuple tup = j.next();
                System.out.println(tup);
            }
            j.close();
            Database.getBufferPool().transactionComplete(tid);

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static Tuple getHeapTuple(int n, int width) {
        Tuple tup = new Tuple(getTupleDesc(width));
        for (int i = 0; i < width; ++i) {
            tup.setField(i, new IntField(n));
        }
        return tup;
    }

    public static TupleDesc getTupleDesc(int n) {
        return new TupleDesc(getTypes(n));
    }

    public static Type[] getTypes(int len) {
        Type[] types = new Type[len];
        for (int i = 0; i < len; ++i) {
            types[i] = Type.INT_TYPE;
        }
        return types;
    }
}
