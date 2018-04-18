package gammasupport;

import static gammasupport.GammaError.mapHasIncorrectLength;
import static gammasupport.GammaConstants.splitLen;
import static gammasupport.GammaConstants.mapSize;;

/** implements Bit Map (BMap) for Gamma */
public class BMap {
    private static char OFF = 'f';
    private static char ON = 't';
    private final boolean[][] map;


    /** make a BMap from a serialized bitmap produced by serialize()
     * @param b -- serialized string bit map
     */
    public BMap(String b) {
        checksize(b);
        map = new boolean[splitLen][mapSize];

        for (int i = 0; i < splitLen; i++) {
            for (int j = 0; j < mapSize; j++) {
                map[i][j] = (b.charAt(index(i, j)) == ON);
            }
        }
    }

     /** make an empty BMap */
    public BMap() {
        map = new boolean[splitLen][mapSize];
        for (int i = 0; i < splitLen; i++) {
            for (int j = 0; j < mapSize; j++) {
                map[i][j] = false;
            }
        }
    }


    /** 
     * static factory used in MMerge
     * @param m1 -- a bitmap
     * @param m2 -- another bitmap
     * @return disjunction of BMaps m1 and m2
     */
    public static BMap or(BMap m1, BMap m2) {
        BMap bm = new BMap();

        for (int i = 0; i < splitLen; i++) {
            for (int j = 0; j < mapSize; j++) {
                bm.map[i][j] = m1.map[i][j] | m2.map[i][j];
            }
        }
        return bm;
    }

    // simple check to see if String is of correct length for a BMap encoding
    private static void checksize(String b) {
        if (b.length() != mapSize * splitLen) {
            throw GammaError.toss(mapHasIncorrectLength,b.length(),mapSize * splitLen);
        }
    }

    private static int index(int i, int j) {
        return i * mapSize + j;
    }
   
    /**
     * given 'this' bitmap, extract the bitmap that represents the mth row.
     * @param m -- which row to extract and fill in the rest with zeros
     * @return -- the bitmap for the mth hash-split of an input stream
     */
    public BMap extract(int m) {
        int j;

        BMap ss = new BMap();
        for (int i = 0; i < splitLen; i++) {
            // copy row i=m -- it is zeroed out otherwise
            if (i == m) { // copy only this row
                for (j = 0; j < mapSize; j++) {
                    ss.map[i][j] = map[i][j];
                }
            }
        }
        return ss;
    }
    //---------------- object methods -------
    
    /**
     * @param x -- string to hash
     * @return --the BMap boolean for the hash of x
     */
    public boolean getBit(String x) {
        int h = hsh(x);
        int i = h % splitLen;
        int j = h % mapSize;
        return map[i][j];
    }

    /**
     * @param x -- set the hash bit for string x
     */
    public void setBit(String x) {
        int h = hsh(x);
        int i = h % splitLen;
        int j = h % mapSize;
        map[i][j] = true;
    }


    /** 
     * @return  serialized string of a bloom filter
     */
    String serialize() {
        char[] array = new char[mapSize * splitLen];

        for (int i = 0; i < splitLen; i++) {
            for (int j = 0; j < mapSize; j++) {
                array[index(i, j)] = map[i][j] ? ON : OFF;
            }
        }
        return new String(array);
    }
    
    /**
     * @return String encoding a bitmap
     */
    @Override
    public String toString() {
        return serialize();
    }
    
    /** 
     * @return  serialized string of a bloom filter
     */
   static BMap unserialize(String encoded) {
        BMap bm = new BMap();

        for (int i = 0; i < splitLen; i++) {
            for (int j = 0; j < mapSize; j++) {
                bm.map[i][j] = encoded.charAt(index(i,j))==ON;
            }
        }
        return bm;
    }

    /**
     * this method takes a joinkey and determines which substream
     * the corresponding tuple is to be mapped.  It is used primarily
     * if not exclusively by SPLIT
     * @param x -- the string to hash
     * @return the hash value of string x
     */
    public static int myhash(String x) {
        int h = hsh(x);
        return h % splitLen;
    }

    private static int hsh(String x) {
        return Math.abs(x.hashCode());
    }
}
