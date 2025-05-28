package company.db;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.stream.Collector;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import db.Aggregation;
import db.Aggregation.AggregateFunction;
import db.Database;
import db.Record;
import db.Table;

/**
 * The {@code Company} class uses a {@code Database} to perform queries related to a company.
 * 
 * @author Jeong-Hyon Hwang (jhh@cs.albany.edu)
 */
public class Company {

	/**
	 * The main method of the {Company} class.
	 * 
	 * @param args
	 *            the program arguments
	 * @throws Exception
	 *             if an error occurs
	 */
	public static void main(String[] args) throws Exception {
		var database = new Database("Sample");
		var schema = database.createTable("projects").attribute("projectName")
				.attribute("budget").key("projectName");
		System.out.println(database);

		database.createTable("employees").attribute("employeeNumber").attribute("zipCode")
				.attribute("projectName").key("employeeNumber");
		System.out.println(database);
		System.out.println();

		Record record = new Record(schema, "P10", 1000000.0);
		System.out.println(record);

		record = new Record(schema, "P11", 2000000.0);
		System.out.println(record);

		Table table = database.table("projects");
		record = table.insertRecord("P10", 1000000.0);
		System.out.println(record);

		try {
			record = table.insertRecord("P10", 2000000.0);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println(database);

		addData(database, 6);
		Stream<Record> result = database.select("*", "projects");
		System.out.println("projects:");
		result.forEach(r -> System.out.println(r));
		System.out.println("");

		result = database.select("*", "employees");
		System.out.println("employees:");
		result.forEach(r -> System.out.println(r));
		System.out.println("");

		result = database.select("*", "projects", "budget > 1000000");
		System.out.println("projects with budget > 1,000,000:");
		result.forEach(r -> System.out.println(r));
		System.out.println("");

		result = database.select("employeeNumber, budget",
				"employees natural join projects");
		System.out.println("employee number, budget");
		result.forEach(r -> System.out.println(r));
		System.out.println("");

		result = database.select("budget", "employees natural join projects",
				"employeeNumber = \"E10\"");
		System.out.println("budget of the project participated by employee E10:");
		result.forEach(r -> System.out.println(r));

		result = database.select("budget", "employees natural join projects",
				"employeeNumber = \"E11\"");
		System.out.println("budget of the project participated by employee E11:");
		result.forEach(r -> System.out.println(r));

		result = database.select("budget", "employees natural join projects",
				"employeeNumber = \"E15\"");
		System.out.println("budget of the project participated by employee E15:");
		result.forEach(r -> System.out.println(r));
		System.out.println("");

		var c = Aggregation.count();
		System.out.println(IntStream.range(0, 1).boxed().collect(c));
		System.out.println(IntStream.range(0, 10).boxed().collect(c));

		Collector<Integer, AggregateFunction<Integer, Integer>, Integer> m = Aggregation
				.maximum();
		System.out.println(List.of(3, 5, 7).stream().collect(m));
		System.out.println(List.of(5, 7, 9).stream().collect(m));
		System.out.println(List.of(9, 5, 7).stream().collect(m));

		result = database.select("count(employeeNumber) as count", "employees");
		System.out.println("number of employees:");
		result.forEach(r -> System.out.println(r));
		System.out.println("");

		result = database.select("max(budget) as maxBudget", "projects");
		System.out.println("maximum of project budgets:");
		result.forEach(r -> System.out.println(r));
		System.out.println("");

		result = database.selectGroupBy("zipCode, count(employeeNumber) as employeeCount",
				"employees", "zipCode");
		System.out.println("ZIP code, number of employees:");
		result.forEach(r -> System.out.println(r));
		System.out.println("");

		result = database.selectGroupBy("budget, count(employeeNumber) as employeeCount",
				"employees natural join projects", "budget");
		System.out.println("budget, number of employees:");
		result.forEach(e -> System.out.println(e));
		System.out.println("");
	}

	/**
	 * Adds data to the specified {@code Database}.
	 * 
	 * @param database
	 *            a {@code Database}
	 * @param projects
	 *            the number of projects
	 */
	public static void addData(Database database, int projects) {
		Table table = database.table("projects");
		BiConsumer<String, Double> addProject = (s, i) -> table.insertRecord(s, i);
		Table referencing = database.table("employees");
		BiConsumer<String, Map.Entry<Integer, String>> addEmployee = (s, e) -> referencing
				.insertRecord(s, e.getKey(), e.getValue());
		addData(projects, addProject, addEmployee);
	}

	/**
	 * Adds data related to {@code Project}s and {@code Employee}s of {@code Company}.
	 * 
	 * @param numberOfProjects
	 *            the number of {@code Project}s of the {@code Company}
	 * @param addProject
	 *            a {@code BiConsumer} for adding each project
	 * @param addEmployee
	 *            a {@code BiConsumer} for adding each employee
	 */
	public static void addData(int numberOfProjects,
			BiConsumer<String, Double> addProject,
			BiConsumer<String, Map.Entry<Integer, String>> addEmployee) {
		double[] balances = { 1000000.0, 2000000.0, 3000000.0 };
		int[] zipCodes = { 12222, 12223, 12224, 12225 };
		int n = 3;
		int digits = (int) Math.ceil(Math.log10(3 * numberOfProjects));
		IntStream.range(0, numberOfProjects).forEach(i -> {
			var projectName = String.format("P%0" + digits + "d", i);
			addProject.accept(projectName, balances[i % balances.length]);
			IntStream.range(0, n).forEach(j -> addEmployee.accept(
					String.format("E%0" + digits + "d", n * i + j),
					Map.entry(zipCodes[(2 * i + j) % zipCodes.length], projectName)));
			if (i == numberOfProjects - 1)
				addEmployee.accept(String.format("E%0" + digits + "d", n * i + n),
						Map.entry(zipCodes[(2 * i + n) % zipCodes.length], projectName));
		});
	}

}
