package gammastream;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import PrologDB.Table;
import PrologDB.TableSchema;
import PrologDB.Tuple;
import gammasupport.BMap;

public class TupleStream implements Stream<Tuple> {
    
    private TableSchema schema;
    private Stream<Tuple> tuples;
    
    public TupleStream(TableSchema schema, Stream<Tuple> stream) {
        this.schema = Objects.requireNonNull(schema);
        this.tuples = Objects.requireNonNull(stream);
    }
    
    public static TupleStream readtable(String filename) {
        Table t = Table.readTable(filename);
        return new TupleStream(t.getSchema(), t.tuples().stream());
    }
    
    public TableSchema getSchema() {
        return schema;
    }
    
    public TupleStream gammaJoin(TupleStream other, String joinkey1, String joinkey2) {
        // if we are parallel, turn on parallel for the other stream
        if (this.tuples.isParallel()) {
            other = other.parallel();
        }
        BMap bitMap = new BMap();
        return this.bloom(bitMap, joinkey1) 
                   .hjoin(other.bfilter(bitMap, joinkey2), joinkey1, joinkey2);
    }
    
    // this is somewhat analogous to normal bloom.
    // Have to accept bitMap to populate as a parameter since
    // streams can not split off into two parts
    public TupleStream bloom(BMap bitMap, String joinKey) {
        return this.peek(t -> bitMap.setBit(t.get(joinKey)));
    }
    
    public TupleStream bfilter(BMap bitMap, String joinkey) {
        return this.filter(t -> bitMap.getBit(t.get(joinkey)));
    }
    
    public TupleStream hjoin(TupleStream other, String joinkey1, String joinkey2) {
        // if we are parallel, turn on parallel for the other stream
        if (this.tuples.isParallel()) {
            other = other.parallel();
        }
        // collectors.groupingby uses a HashMap which is what we want
        Map<String, List<Tuple>> map = this.tuples.collect(Collectors.groupingBy(t -> t.get(joinkey1)));
        
        // create joinTable Schema
        TableSchema joinSchema = this.schema.crossProduct(other.schema);
        
        // use other.myTuples directly to avoid piling on wrappers of TupleStream
        return new TupleStream(joinSchema, other.tuples.filter(t2 -> map.containsKey(t2.get(joinkey2)))
                                                         .flatMap(t2 -> map.get(t2.get(joinkey2)).stream()
                                                                           .map(t1 -> new Tuple(joinSchema)
                                                                                          .setValues(t1)
                                                                                          .setValues(t2))));
    }
    
    // methods from Stream interface
    // just delegate to stream field

    @Override
    public void close() {
        tuples.close();
    }

    @Override
    public boolean isParallel() {
        return tuples.isParallel();
    }

    @Override
    public Iterator<Tuple> iterator() {
        return tuples.iterator();
    }

    @Override
    public TupleStream onClose(Runnable arg0) {
        return new TupleStream(schema, tuples.onClose(arg0));
    }

    @Override
    public TupleStream parallel() {
        return new TupleStream(schema, tuples.parallel());
    }

    @Override
    public TupleStream sequential() {
        return new TupleStream(schema, tuples.sequential());
    }

    @Override
    public Spliterator<Tuple> spliterator() {
        return tuples.spliterator();
    }

    @Override
    public TupleStream unordered() {
        return new TupleStream(schema, tuples.unordered());
    }

    @Override
    public boolean allMatch(Predicate<? super Tuple> arg0) {
        return tuples.allMatch(arg0);
    }

    @Override
    public boolean anyMatch(Predicate<? super Tuple> arg0) {
        return tuples.anyMatch(arg0);
    }

    @Override
    public <R, A> R collect(Collector<? super Tuple, A, R> arg0) {
        return tuples.collect(arg0);
    }

    @Override
    public <R> R collect(Supplier<R> arg0, BiConsumer<R, ? super Tuple> arg1,
                         BiConsumer<R, R> arg2) {
        return tuples.collect(arg0, arg1, arg2);
    }

    @Override
    public long count() {
        return tuples.count();
    }

    @Override
    public TupleStream distinct() {
        return new TupleStream(schema, tuples.distinct());
    }

    @Override
    public TupleStream filter(Predicate<? super Tuple> arg0) {
        return new TupleStream(schema, tuples.filter(arg0));
    }

    @Override
    public Optional<Tuple> findAny() {
        return tuples.findAny();
    }

    @Override
    public Optional<Tuple> findFirst() {
        return tuples.findFirst();
    }

    @Override
    public <R> Stream<R> flatMap(Function<? super Tuple, ? extends Stream<? extends R>> arg0) {
        return tuples.flatMap(arg0);
    }

    @Override
    public DoubleStream flatMapToDouble(Function<? super Tuple, ? extends DoubleStream> arg0) {
        return tuples.flatMapToDouble(arg0);
    }

    @Override
    public IntStream flatMapToInt(Function<? super Tuple, ? extends IntStream> arg0) {
        return tuples.flatMapToInt(arg0);
    }

    @Override
    public LongStream flatMapToLong(Function<? super Tuple, ? extends LongStream> arg0) {
        return tuples.flatMapToLong(arg0);
    }

    @Override
    public void forEach(Consumer<? super Tuple> arg0) {
        tuples.forEach(arg0);
    }

    @Override
    public void forEachOrdered(Consumer<? super Tuple> arg0) {
        tuples.forEachOrdered(arg0);
    }

    @Override
    public TupleStream limit(long arg0) {
        return new TupleStream(schema, tuples.limit(arg0));
    }

    @Override
    public <R> Stream<R> map(Function<? super Tuple, ? extends R> arg0) {
        return tuples.map(arg0);
    }

    @Override
    public DoubleStream mapToDouble(ToDoubleFunction<? super Tuple> arg0) {
        return tuples.mapToDouble(arg0);
    }

    @Override
    public IntStream mapToInt(ToIntFunction<? super Tuple> arg0) {
        return tuples.mapToInt(arg0);
    }

    @Override
    public LongStream mapToLong(ToLongFunction<? super Tuple> arg0) {
        return tuples.mapToLong(arg0);
    }

    @Override
    public Optional<Tuple> max(Comparator<? super Tuple> arg0) {
        return tuples.max(arg0);
    }

    @Override
    public Optional<Tuple> min(Comparator<? super Tuple> arg0) {
        return tuples.min(arg0);
    }

    @Override
    public boolean noneMatch(Predicate<? super Tuple> arg0) {
        return noneMatch(arg0);
    }

    @Override
    public TupleStream peek(Consumer<? super Tuple> arg0) {
        return new TupleStream(schema, tuples.peek(arg0));
    }

    @Override
    public Optional<Tuple> reduce(BinaryOperator<Tuple> arg0) {
        return tuples.reduce(arg0);
    }

    @Override
    public Tuple reduce(Tuple arg0, BinaryOperator<Tuple> arg1) {
        return tuples.reduce(arg0, arg1);
    }

    @Override
    public <U> U reduce(U arg0, BiFunction<U, ? super Tuple, U> arg1,
                        BinaryOperator<U> arg2) {
        return tuples.reduce(arg0, arg1, arg2);
    }

    @Override
    public TupleStream skip(long arg0) {
        return new TupleStream(schema, tuples.skip(arg0));
    }

    @Override
    public TupleStream sorted() {
        return new TupleStream(schema, tuples.sorted());
    }

    @Override
    public Stream<Tuple> sorted(Comparator<? super Tuple> arg0) {
        return new TupleStream(schema, tuples.sorted(arg0));
    }

    @Override
    public Object[] toArray() {
        return tuples.toArray();
    }

    @Override
    public <A> A[] toArray(IntFunction<A[]> arg0) {
        return tuples.toArray(arg0);
    }
}
