package gammastream;

import gammasupport.BMap;

public class Gamma {

    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        gammaJoin("RelationData/client.pl", "RelationData/viewing.pl", "cno", "cno");
        long end = System.currentTimeMillis();System.out.printf("Running time of non-parallel version: %d milliseconds\n\n", end - start);
        
        start = System.currentTimeMillis();
        parallelGammaJoin("RelationData/client.pl", "RelationData/viewing.pl", "cno", "cno");
        end = System.currentTimeMillis();
        System.out.printf("Running time of parallel version: %d milliseconds\n", end - start);
    }
    
    // non-parallel gamma join (i.e. gamma without HSplit and Merge)
    public static void gammaJoin(String filename1, String filename2, String joinkey1, String joinkey2) {
        BMap bitMap = new BMap();
        TupleStream.readtable(filename1)
                   .bloom(bitMap, joinkey1)
                   .hjoin(TupleStream.readtable(filename2)
                                     .bfilter(bitMap, joinkey2), joinkey1, joinkey2)
                   .collect(CollectableTable.toTable())
                   .print();
    }
    
    // this is as close to the full gamma circuit described in the lecture notes as I think the java
    // streams paradigm can get. The call to parallel will split and merge the stream in order to
    // parallelize the stream. While not strictly the same as HSplit (which splits based off a hash),
    // I think this is as close as it can get.
    public static void parallelGammaJoin(String filename1, String filename2, String joinkey1, String joinkey2) {
        BMap bitMap = new BMap();
        TupleStream.readtable(filename1)
                   .parallel()
                   .bloom(bitMap, joinkey1) 
                   .hjoin(TupleStream.readtable(filename2)
                                     .parallel()
                                     .bfilter(bitMap, joinkey2), joinkey1, joinkey2)
                   .collect(CollectableTable.toTable())
                   .print();
    }
}
