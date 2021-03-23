package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    int gbfield;
    Type gbfieldtype;
    int afield; 
    Op what;
    
    Map<Field, Integer> map = new HashMap<Field, Integer>();
    
    TupleDesc tupleDesc;
    
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	this.what = what;
    	
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
		Field gbfieldInstance = null;
		if(gbfield == Aggregator.NO_GROUPING) {
			gbfieldInstance = new IntField(Aggregator.NO_GROUPING);
		} else {
			gbfieldInstance = tup.getField(gbfield);
		}

    	Integer result = map.get(gbfieldInstance);
    	Integer value = null; 
    	switch (what) {
		case COUNT:
			if (result == null) {
				value = 1;
			} else {
				value = result + 1;
			}
			break;

		default:
			break;
		}
    	map.put(gbfieldInstance, value);
    	
    	if(tupleDesc == null) {
    		tupleDesc = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});  
    	}
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
    	List<Tuple> tuples = new ArrayList<>();
    	map.forEach((key,value) -> {
    		Tuple tuple = new Tuple(tupleDesc);
    		tuple.setField(0, key); 
    		tuple.setField(1,  new IntField(value));
    		tuples.add(tuple);
    	});
    	
    	TupleIterator tupleIterator = new TupleIterator(tupleDesc, tuples);
    	return tupleIterator;
    	
    }

}
