package db;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An {@code Aggregation} can group {@code Record}s by certain attributes (e.g., ZIP code, gender) and obtain aggregate
 * values (e.g., maximum, average, and count) for each group of {@code Record}s.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
public class Aggregation extends UnaryOperator {

	/**
	 * A map that associates the name of each {@code AggregateFunction} with the corresponding {@code Collector}.
	 */
	public static Map<String, Collector<?, ? extends AggregateFunction<?, ?>, ?>> name2aggregateFunction = Map
			.of("count", count(), "sum", sum(), "min", minimum(), "max", maximum(), "avg",
					average());

	/**
	 * Provides a {@code List} containing the name of each aggregate function.
	 * 
	 * @return a {@code List} containing the name of each aggregate function
	 */
	static List<String> aggregateFunctionNames() {
		return name2aggregateFunction.keySet().stream().toList();
	}

	/**
	 * The output schema of this {@code Aggregation}.
	 */
	TableSchema outputSchema = new TableSchema();

	/**
	 * The names of the grouping attributes.
	 */
	String[] groupingAttributes = new String[0];

	/**
	 * The names of the attributes to which {@code AggregateFunction}s are applied.
	 */
	List<String> inputAttributeNames = new ArrayList<String>();

	/**
	 * The {@code Collector}s used for aggregation.
	 */
	private CompositeCollector collectors = new CompositeCollector();

	/**
	 * Constructs an {@code Aggregation}.
	 * 
	 * @param input
	 *            the input {@code Operator} for the {@code Aggregation}
	 * @param groupingAttributes
	 *            the names of the grouping attributes
	 * @param aggregationDescriptions
	 *            descriptions of aggregation functions applied
	 */
	public Aggregation(Operator input, List<String> groupingAttributes,
			Map<String, String> aggregationDescriptions) {
		super(input);
		this.groupingAttributes = groupingAttributes.toArray(new String[0]);
		if (groupingAttributes != null)
			for (var groupingAttribute : groupingAttributes)
				outputSchema.attribute(groupingAttribute);
		for (Map.Entry<String, String> aggregationDescription : aggregationDescriptions
				.entrySet()) {
			outputSchema.attribute(aggregationDescription.getValue());
			String d = aggregationDescription.getKey();
			var functionName = d.substring(0, d.indexOf('(')).trim();
			var inputAttributeName = d.substring(d.indexOf('(') + 1, d.indexOf(')'))
					.trim();
			inputAttributeNames.add(inputAttributeName);
			Collector<?, ? extends AggregateFunction<?, ?>, ?> a = name2aggregateFunction
					.get(functionName);
			collectors.add(a);
		}
	}

	/**
	 * Returns the output schema of this {@code Aggregation}.
	 * 
	 * @return the output schema of this {@code Aggregation}
	 */
	@Override
	public TableSchema outputSchema() {
		return outputSchema;
	}

	/**
	 * Returns the output {@code Stream<Record>} of this {@code Aggregation}.
	 * 
	 * @return the output {@code Stream<Record>} of this {@code Aggregation}
	 */
	@Override
	public Stream<Record> stream() {
		Function<Record, List<Object>> classifier = r -> r.values(groupingAttributes);
		Map<List<Object>, List<Object>> summary = input.stream()
				.collect(Collectors.groupingBy(classifier, collectors));
		return summary.entrySet().stream()
				.map(e -> outputRecord(e.getKey(), e.getValue()));
	}

	/**
	 * Constructs a {@code Record} from the specified grouping attribute values and {@code AggregateFunction}s.
	 * 
	 * @param groupingAttributeValues
	 *            a {@code List} of grouping attribute values
	 * @param aggregateFunctions
	 *            a {@code List} of {@code AggregateFunction}s
	 * @return a {@code Record} constructed from the specified grouping attribute values and {@code AggregateFunction}s
	 */
	private Record outputRecord(List<Object> groupingAttributeValues,
			List<Object> aggregateValues) {
		var attributeValues = new ArrayList<Object>(groupingAttributeValues);
		attributeValues.addAll(aggregateValues);
		return new Record(outputSchema, attributeValues);
	}

	/**
	 * A {@code CompositeCollector} is a {@code Collector} that can apply multiple {@code AggregateFunction}s to input
	 * attributes.
	 * 
	 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
	 */
	class CompositeCollector implements
			Collector<Record, List<? extends AggregateFunction<?, ?>>, List<Object>> {

		/**
		 * The {@code Collector}s used by this {@code CompositeCollector}.
		 */
		List<Collector<?, ? extends AggregateFunction<?, ?>, ?>> collectors = new ArrayList<Collector<?, ? extends AggregateFunction<?, ?>, ?>>();

		/**
		 * Adds the specified {@code Collector} to this {@code CompositeCollector}.
		 * 
		 * @param collector
		 *            a {@code Collector}
		 */
		public void add(Collector<?, ? extends AggregateFunction<?, ?>, ?> collector) {
			this.collectors.add(collector);
		}

		@Override
		public Supplier<List<? extends AggregateFunction<?, ?>>> supplier() {
			return () -> collectors.stream().map(c -> c.supplier().get()).toList();
		}
		@Override
		public BiConsumer<List<? extends AggregateFunction<?, ?>>, Record> accumulator() {
		    return (aggregates, record) -> {
		        for (int i = 0; i < collectors.size(); i++) {
		            
		            Object value = record.value(inputAttributeNames.get(i));
		            
		            // Use a helper method to perform a safe cast and update the aggregate function
		            updateAggregate(aggregates.get(i), value);
		        }
		    };
		}

		// Helper method to perform a safe cast and update the aggregate function
		@SuppressWarnings("unchecked")
		private <T> void updateAggregate(AggregateFunction<T, ?> aggregateFunction, Object value) {
		    // Cast the value to the expected type of the aggregate function and update
		    aggregateFunction.update((T) value);
		}



		@Override
		public BinaryOperator<List<? extends AggregateFunction<?, ?>>> combiner() {
		    return (a1, a2) -> {
		        for (int i = 0; i < a1.size(); i++) {
		            // Combine aggregate functions safely
		            mergeAggregates(a1.get(i), a2.get(i));
		        }
		        return a1;
		    };
		}

		// Helper method to combine two aggregate functions
		@SuppressWarnings("unchecked")
		private <T> void mergeAggregates(AggregateFunction<T, ?> agg1, AggregateFunction<T, ?> agg2) {
		    // Call the merge method or appropriate logic to combine them
		 
		}


		@Override
		public Function<List<? extends AggregateFunction<?, ?>>, List<Object>> finisher() {
			return a -> {
				Stream<Object> s = a.stream().map(e -> e.result());
				return s.toList();
			};

		}

		@Override
		public Set<Characteristics> characteristics() {
			return Set.of();
		}

	}

	/**
	 * An {@code AggregateFunction} computes a summary value (e.g., maximum, minimum, count, and average) over a set of
	 * values.
	 * 
	 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
	 * 
	 * @param <T>
	 *            the type of the values
	 * @param <R>
	 *            the result type
	 */
	public static abstract class AggregateFunction<T, R> {

		/**
		 * Updates this {@code AggregateFunction} based on the specified value.
		 * 
		 * @param v
		 *            a value
		 * @return this {@code AggregateFunction}
		 */
		public abstract AggregateFunction<T, R> update(T v);

		/**
		 * Updates this {@code AggregateFunction} based on the specified {@code AggregateFunction}.
		 * 
		 * @param a
		 *            an {@code AggregateFunction}
		 * @return this {@code AggregateFunction}
		 */
		public abstract AggregateFunction<T, R> update(AggregateFunction<T, R> a);

		/**
		 * Returns the result of this {@code AggregateFunction}.
		 * 
		 * @return the result of this {@code AggregateFunction}
		 */
		public abstract R result();
	}

	/**
	 * A {@code Count} computes, given a collection of values, the count of the values.
	 * 
	 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
	 */
	public static class Count<T> extends AggregateFunction<T, Integer> {

		/**
		 * The count managed by this {@code Count}.
		 */
		protected int count = 0;

		@Override
		public AggregateFunction<T, Integer> update(T v) {
			count++;
			return this;
		}

		@Override
		public AggregateFunction<T, Integer> update(AggregateFunction<T, Integer> a) {
			count += ((Count<T>) a).count;
			return this;
		}

		@Override
		public Integer result() {
			return count;
		}

	}

	/**
	 * A {@code Sum} computes, given a collection of values, the sum of the values.
	 * 
	 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
	 */
	public static class Sum extends AggregateFunction<Number, Number> {

		/**
		 * The sum maintained by this {@code Sum}.
		 */
		Number sum = null;

		@Override
		public AggregateFunction<Number, Number> update(Number v) {
			if (sum == null)
				sum = v;
			else if (v instanceof Integer && sum instanceof Integer)
				sum = v.intValue() + sum.intValue();
			else
				sum = v.doubleValue() + sum.doubleValue();
			return this;
		}

		@Override
		public AggregateFunction<Number, Number> update(
				AggregateFunction<Number, Number> a) {
			this.update(((Sum) a).sum);
			return this;
		}

		@Override
		public Number result() {
			return sum;
		}

	}

	/**
	 * A {@code Average} computes, given a collection of values, the average of the values.
	 * 
	 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
	 */
	public static class Average extends Sum {

		/**
		 * The count.
		 */
		int count = 0;

		@Override
		public AggregateFunction<Number, Number> update(Number v) {
		    if (sum == null) {
		        sum = v;
		        count = 1; // Count starts at 1 since we're adding the first element
		    } else {
		        sum = sum instanceof Integer ? sum.intValue() + v.intValue() : sum.doubleValue() + v.doubleValue();
		        count++;
		    }
		    return this;
		}
		@Override
		public AggregateFunction<Number, Number> update(AggregateFunction<Number, Number> a) {
		    Average other = (Average) a;
		    if (this.sum == null) {
		        this.sum = other.sum;
		        this.count = other.count;
		    } else {
		        this.sum = this.sum instanceof Integer ? this.sum.intValue() + other.sum.intValue() : this.sum.doubleValue() + other.sum.doubleValue();
		        this.count += other.count;
		    }
		    return this;
		}

		@Override
		public Number result() {
			if (sum instanceof Integer)
				return sum.intValue() / count;
			else
				return sum.doubleValue() / count;
		}

	}

	/**
	 * A {@code Maximum} computes, given a collection of values, the maximum of the values.
	 * 
	 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
	 */
	public static class Maximum<T extends Comparable<T>> extends AggregateFunction<T, T> {

		/**
		 * The current maximum value.
		 */
		protected T maximum = null;

		/**
		 * Updates this {@code Maximum} based on the specified value.
		 * 
		 * @param v
		 *            a value for updating this {@code Maximum}
		 */
		@Override
		public AggregateFunction<T, T> update(T v) {
			if (maximum == null || maximum.compareTo(v) < 0)
				maximum = v;
			return this;
		}

		@Override
		public AggregateFunction<T, T> update(AggregateFunction<T, T> a) {
			return update(((Maximum<T>) a).maximum);
		}

		@Override
		public T result() {
			return maximum;
		}

	}

	/**
	 * A {@code Minimum} computes, given a collection of values, the minimum of the values.
	 * 
	 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
	 */
	public static class Minimum<T extends Comparable<T>> extends AggregateFunction<T, T> {

		/**
		 * The current maximum value.
		 */
		protected T minimum = null;

		/**
		 * Updates this {@code Minimum} based on the specified value.
		 * 
		 * @param v
		 *            a value for updating this {@code Maximum}
		 */
		@Override
		public AggregateFunction<T, T> update(T v) {
			if (minimum == null || minimum.compareTo(v) > 0)
				minimum = v;
			return this;
		}

		@Override
		public AggregateFunction<T, T> update(AggregateFunction<T, T> a) {
			return update(((Minimum<T>) a).minimum);
		}

		@Override
		public T result() {
			return minimum;
		}

	}

	/**
	 * Constructs a {@code Collector} that applies {@code Count}.
	 * 
	 * @param <T>
	 *            the type of the values to be given to {@code Count}
	 * @return a {@code Collector} that applies {@code Count}
	 */
	public static <T> Collector<T, AggregateFunction<T, Integer>, Integer> count() {
		return collector(Count<T>::new);
	}

	/**
	 * Constructs a {@code Collector} that applies {@code Sum}.
	 * 
	 * @return a {@code Collector} that applies {@code Sum}
	 */
	public static Collector<Number, AggregateFunction<Number, Number>, Number> sum() {
		return collector(Sum::new);
	}

	/**
	 * Constructs a {@code Collector} that applies {@code Average}.
	 * 
	 * @return a {@code Collector} that applies {@code Average}
	 */
	public static Collector<Number, AggregateFunction<Number, Number>, Number> average() {
		return collector(Average::new);
	}

	/**
	 * Constructs a {@code Collector} that applies {@code Maximum}.
	 * 
	 * @param <T>
	 *            the type of the values to be given to {@code Maximum}
	 * @return a {@code Collector} that applies {@code Maximum}
	 */
	public static <T extends Comparable<T>> Collector<T, AggregateFunction<T, T>, T> maximum() {
		return collector(Maximum::new);
	}

	/**
	 * Constructs a {@code Collector} that applies {@code Minimum}.
	 * 
	 * @param <T>
	 *            the type of the values to be given to {@code Minimum}
	 * @return a {@code Collector} that applies {@code Minimum}
	 */
	public static <T extends Comparable<T>> Collector<T, AggregateFunction<T, T>, T> minimum() {
		return collector(Minimum::new);
	}

	/**
	 * Constructs a {@code Collector} that applies an {@code AggregateFunction}
	 * 
	 * @param <T>
	 *            the type of the values to be given to the {@code AggregateFunction}
	 * @param <R>
	 *            the type of the result of the {@code AggregateFunction}
	 * @param supplier
	 *            a {@code Supplier} that can construct {@code AggregateFunction}s
	 * @return the constructed {@code Collector}
	 */
	private static <T, R> Collector<T, AggregateFunction<T, R>, R> collector(
			Supplier<AggregateFunction<T, R>> supplier) {
		return new Collector<T, AggregateFunction<T, R>, R>() {

			@Override
			public Supplier<AggregateFunction<T, R>> supplier() {
				return supplier;
			}

			@Override
			public BiConsumer<AggregateFunction<T, R>, T> accumulator() {
				return (a, v) -> a.update(v);
			}

			@Override
			public BinaryOperator<AggregateFunction<T, R>> combiner() {
				return (a1, a2) -> a1.update(a2);
			}

			@Override
			public Function<AggregateFunction<T, R>, R> finisher() {
				return (a) -> a.result();
			}

			@Override
			public Set<Characteristics> characteristics() {
				return Set.of();
			}

		};
	}}

