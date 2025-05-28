package db;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A {@code NaturalJoin} finds, for each given {@code Record}, every matching {@code Record} from a {@code Table} and
 * then produces a concatenation of these two {@code Record}s.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
public class NaturalJoin extends UnaryOperator {

    /**
     * The common attributes between the two {@code TableSchema}s involved in this {@code NaturalJoin}.
     */
    Set<String> commonAttributes;

    /**
     * The output {@code TableSchema} of this {@code NaturalJoin}.
     */
    TableSchema outputSchema;

    /**
     * The referenced {@code Table} for this {@code NaturalJoin}.
     */
    Table referencedTable;

    /**
     * Constructs a {@code NaturalJoin}.
     * 
     * @param input          the input {@code Operator} for the {@code NaturalJoin}
     * @param referencedTable the referenced {@code Table} for the {@code NaturalJoin}
     */
    public NaturalJoin(Operator input, Table referencedTable) {
        super(input);
        this.referencedTable = referencedTable;
        this.commonAttributes = input.outputSchema().commonAttributeNames(referencedTable.schema());
        this.outputSchema = new TableSchema(input.outputSchema(), referencedTable.schema());
    }

    @Override
    public TableSchema outputSchema() {
        return outputSchema;
    }

    @Override
    public Stream<Record> stream() {
        Stream<Record> inputStream = input.stream(); //stream of records retrieved
        return inputStream.flatMap(inputRecord -> {
            Collection<Record> matchingRecords = referencedTable.matchingRecords(inputRecord, commonAttributes);//record from ref table are gotten from the commonality between the record and table
            return matchingRecords.stream().map(matchingRecord -> {
                return Record.concatenate(inputRecord, matchingRecord, outputSchema);
            });
        });
    }
   
    private Set<Object> keyValues(Record inputRecord) {//this method is help method
        return commonAttributes.stream()
                .map(inputRecord::value)
                .collect(Collectors.toSet()); }}
