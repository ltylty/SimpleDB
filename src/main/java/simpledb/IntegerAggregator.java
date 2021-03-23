package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    int gbfield;
    Type gbfieldtype;
    int afield;
    Op what;
    
    private HashMap<Field, Integer> map;
    private HashMap<Field, Integer> countMap;
    private HashMap<Field, Integer> sumMap;
    
    private TupleDesc td;
    
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	this.what = what;
    	
    	this.map = new HashMap<Field, Integer>();
    	this.countMap = new HashMap<Field, Integer>();
    	this.sumMap = new HashMap<Field, Integer>();
    	
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
		Field gbfieldInstance = null;
    	if (gbfield == Aggregator.NO_GROUPING) {
			gbfieldInstance = new IntField(Aggregator.NO_GROUPING);
		} else {
			gbfieldInstance = tup.getField(gbfield);
		}

		Field afieldInstance = tup.getField(afield);
    	int intValue = ((IntField)afieldInstance).getValue();
    	
    	Integer value = null;
    	Integer result = map.get(gbfieldInstance);
    	switch (what) {
			case SUM: {
				if(result == null) {
					value = intValue;
				} else {
					value = result + intValue;	
				}
				break;
			}
			case MAX: {
				if(result == null) {
					value = intValue;
				} else {
					value = intValue>result ? intValue : result;
				}
				break;
			}
			case MIN: {
				if(result == null) {
					value = intValue;
				} else {
					value = intValue<result ? intValue : result;
				}
				break;
			}
			case COUNT: {
				if(result == null) {
					value = 1;
				} else {
					value = ++result;
				}
				break;
			}
			case AVG: {
				if(sumMap.get(gbfieldInstance) == null) {
					value = intValue;
					countMap.put(gbfieldInstance, 1);
					sumMap.put(gbfieldInstance, intValue);
				} else {
					countMap.put(gbfieldInstance, countMap.get(gbfieldInstance)+1);
					sumMap.put(gbfieldInstance, sumMap.get(gbfieldInstance)+intValue);
					value = sumMap.get(gbfieldInstance)/countMap.get(gbfieldInstance);
				}
				break;
			}
		}
    	map.put(gbfieldInstance, value);
    	
    	if (td == null) {
            td = makeTupleDesc(tup);
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
    	 ArrayList<Tuple> arrayList = new ArrayList<Tuple>();
         for (Field f : map.keySet()) {
             Tuple tuple = new Tuple(td);
             if (gbfield == Aggregator.NO_GROUPING) {
                 tuple.setField(0, new IntField(map.get(f)));
             } else {
                 tuple.setField(0, f);
                 tuple.setField(1, new IntField(map.get(f)));
             }
             arrayList.add(tuple);
         }
         return new TupleIterator(td, arrayList);
    }
    
    private TupleDesc makeTupleDesc(Tuple tuple) {
        Type[] typeAr;
        String[] fieldAr;
        if (gbfield == Aggregator.NO_GROUPING) {
            typeAr = new Type[]{Type.INT_TYPE};
            fieldAr = new String[]{tuple.getTupleDesc().getFieldName(afield)};
        } else {
            typeAr = new Type[2];
            fieldAr = new String[2];
            typeAr[0] = tuple.getTupleDesc().getFieldType(gbfield);
            fieldAr[0] = tuple.getTupleDesc().getFieldName(gbfield);
            typeAr[1] = Type.INT_TYPE;
            fieldAr[1] = tuple.getTupleDesc().getFieldName(afield);
        }
        return new TupleDesc(typeAr, fieldAr);
    }

}
