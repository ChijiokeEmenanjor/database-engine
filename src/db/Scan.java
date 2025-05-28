package db;

import java.util.stream.Stream;

/**
 * A {@code Scan} can access a {@code Table} and provide a {@code Stream} of {@code Record}s that are stored in that
 * {@code Table}.
 * 
 * author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
public class Scan extends Operator {

    /**
     * The {@code Table} that this {@code Scan} accesses.
     */
    Table table;

    /**
     * Constructs a {@code Scan}.
     * 
     * @param table the {@code Table} that the {@code Scan} needs to access
     */
    public Scan(Table table) {
        this.table = table;
    }

    /**
     * Returns the {@code TableSchema} of the {@code Table} that this {@code Scan} accesses.
     * 
     * @return the {@code TableSchema} of the {@code Table} that this {@code Scan} accesses
     */
    @Override
    public TableSchema outputSchema() {
        return table.schema();
    }

    /**
     * Returns a {@code Stream} of the {@code Record}s from the {@code Table} that this {@code Scan} accesses.
     * 
     * @return a {@code Stream} of the {@code Record}s from the {@code Table} that this {@code Scan} accesses
     */
    @Override
    public Stream<Record> stream() {
        return table.getRecords().stream(); // stream of records is retrieved
    }
}
