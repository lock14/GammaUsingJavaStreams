package gammastream;

public class Gamma {

    public static void main(String[] args) {
        // non-parallel gamma join (i.e. gamma without HSplit and Merge)
        long start = System.currentTimeMillis();
        TupleStream.readtable("RelationData/client.pl")
                   .gammaJoin(TupleStream.readtable("RelationData/viewing.pl"),  "cno", "cno")
                   .collect(CollectableTable.toTable())
                   .print();
        long end = System.currentTimeMillis();
        System.out.printf("Running time of non-parallel version: %d milliseconds\n\n", end - start);
        
        // this next one is as close to the full gamma circuit described in the lecture notes
        // as I think the java streams paradigm can get. The call to parallel will split and
        // merge the stream in order to parallelize the stream. While not strictly the same
        // as HSplit (which splits based off a hash), I think this is as close as it can get.
        start = System.currentTimeMillis();
        TupleStream.readtable("RelationData/client.pl")
                   .parallel()
                   .gammaJoin(TupleStream.readtable("RelationData/viewing.pl"),  "cno", "cno")
                   .collect(CollectableTable.toTable())
                   .print();
        end = System.currentTimeMillis();
        System.out.printf("Running time of parallel version: %d milliseconds\n\n", end - start);
        
        //let's finish off by trying a parallel three-way join
        start = System.currentTimeMillis();
        TupleStream.readtable("RelationData/odetails.pl")
                   .parallel()
                   .gammaJoin(TupleStream.readtable("RelationData/orders.pl"),  "ono", "ono")
                   .gammaJoin(TupleStream.readtable("RelationData/parts.pl"), "odetails.pno", "pno")
                   .collect(CollectableTable.toTable())
                   .print();
        end = System.currentTimeMillis();
        System.out.printf("Running time of parallel three-way join: %d milliseconds\n", end - start);
    }
}
