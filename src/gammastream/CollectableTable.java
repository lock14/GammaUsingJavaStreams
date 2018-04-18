package gammastream;

import java.util.stream.Collector;

import PrologDB.Table;
import PrologDB.Tuple;

// MDELite Table class does not support Empty constructor, nor the
// ability to set a schema after construction. This wrapper class
// helps alleviate the issue. Support for this kind of operation
// on the Table class directly would be a nice feature for a
// future version of MDELite

public class CollectableTable {
    private Table table;
    
    public CollectableTable() {
        table = null;
    }
    
    public CollectableTable add(Tuple t) {
        if (table == null) {
            table = new Table(t.getSchema());
        }
        table.add(t);
        return this;
    }
    
    public CollectableTable addTuples(CollectableTable other) {
        if (table == null) {
            return other;
        } else if (other != null && other.table != null) {
            table.addTuples(other.table);
        }
        return this;
    }
    
    public void print() {
        if (table == null) {
            System.out.println("No Table");
        } else {
            table.print();
        }
    }
    
    public static Collector<Tuple, CollectableTable, CollectableTable> toTable() {
        return Collector.of(CollectableTable::new, 
                            CollectableTable::add,
                            (t1, t2) -> t1.addTuples(t2));
    }
}
