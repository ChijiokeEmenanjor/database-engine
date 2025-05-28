package company.db;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import org.junit.Test;

import db.Aggregation;
import db.Database;
import db.TableSchema;
import db.Record;
import db.Table;
import db.Table.DuplicateKeyException;
import db.TableSchema.DuplicateAttributeNameException;



public class UnitTests {

	/**
	 * A {@code Database} used for unit tests.
	 */
	Database database;

	/**
	 * A {@code TableSchema} used for unit tests.
	 */
	TableSchema schema;

	{
		database = new Database("Sample");
		schema = database.createTable("projects").attribute("projectName")
				.attribute("budget").key("projectName");
		database.createTable("employees").attribute("employeeNumber").attribute("zipCode")
				.attribute("projectName").key("employeeNumber");
		try {
			Company.addData(database, 6);
		} catch (Exception e) {
		}
	}

	/**
	 * Tests the Task 1 implementation.
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	@Test
	public void task1() throws Exception {
		var schema = new TableSchema();
		schema.attribute("projectNumber");
		assertEquals("{attributes={projectNumber=0}, key=[]}", "" + schema);
		schema.attribute("budget");
		assertEquals("{attributes={projectNumber=0, budget=1}, key=[]}", "" + schema);
		assertThrows(DuplicateAttributeNameException.class,
				() -> schema.attribute("budget"));
		schema.key("projectNumber");
		assertEquals("{attributes={projectNumber=0, budget=1}, key=[projectNumber]}",
				"" + schema);
	}

	/**
	 * Tests the Task 2 implementation.
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	@Test
	public void task2() throws Exception {
		Record r = new Record(schema, "P10", 1000000.0);
		assertEquals("{projectName=P10, budget=1000000.0}", "" + r);
		r = new Record(schema, "P11", 2000000.0);
		assertEquals("{projectName=P11, budget=2000000.0}", "" + r);
	}

	/**
	 * Tests the Task 3 implementation.
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	@Test
	public void task3() throws Exception {
		var database = new Database("Sample");
		database.createTable("projects").attribute("projectNumber").attribute("budget")
				.key("projectNumber");
		Table table = database.table("projects");
		Record r = table.insertRecord("P10", 1000000.0);
		assertEquals("{projectNumber=P10, budget=1000000.0}", "" + r);
		assertThrows(DuplicateKeyException.class,
				() -> table.insertRecord("P10", 1000000.0));
		assertEquals(
				"Sample{projects={attributes={projectNumber=0, budget=1}, key=[projectNumber]}:1}",
				"" + database);
		table.insertRecord("P11", 2000000.0);
		assertEquals(
				"Sample{projects={attributes={projectNumber=0, budget=1}, key=[projectNumber]}:2}",
				"" + database);
	}

	/**
	 * Tests the Task 4 implementation.
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	@Test
	public void task4() throws Exception {
		var result = database.select("*", "projects").toList();
		assertEquals(6, result.size());
		assertEquals("{projectName=P00, budget=1000000.0}", "" + result.get(0));
		assertEquals("{projectName=P05, budget=3000000.0}", "" + result.get(5));
		result = database.select("*", "employees").toList();
		assertEquals(19, result.size());
		assertEquals("{employeeNumber=E00, zipCode=12222, projectName=P00}",
				"" + result.get(0));
		assertEquals("{employeeNumber=E09, zipCode=12224, projectName=P03}",
				"" + result.get(9));
	}

	/**
	 * Tests the Task 5 implementation.
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	@Test
	public void task5() throws Exception {
		Stream<Record> result = database.select("*", "projects", "budget > 1000000");
		var l = result.toList();
		assertEquals(4, l.size());
		assertEquals("{projectName=P01, budget=2000000.0}", "" + l.get(0));
		assertEquals("{projectName=P05, budget=3000000.0}", "" + l.get(3));
	}

	/**
	 * Tests the Task 6 implementation.
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	@Test
	public void task6() throws Exception {
		Stream<Record> result = database.select("employeeNumber, budget",
				"employees natural join projects");
		var l = result.toList();
		assertEquals(19, l.size());
		assertEquals("{employeeNumber=E00, budget=1000000.0}", "" + l.get(0));
		assertEquals("{employeeNumber=E18, budget=3000000.0}", "" + l.get(18));

		result = database.select("budget", "employees natural join projects",
				"employeeNumber = \"E10\"");
		assertEquals("[{budget=1000000.0}]", "" + result.toList());
		result = database.select("budget", "employees natural join projects",
				"employeeNumber = \"E11\"");
		assertEquals("[{budget=1000000.0}]", "" + result.toList());
		result = database.select("budget", "employees natural join projects",
				"employeeNumber = \"E15\"");
		assertEquals("[{budget=3000000.0}]", "" + result.toList());
	}

	/**
	 * Tests the Task 7 implementation.
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */

	/**
	 * Tests the Task 7 implementation.
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	@Test
	public void task7b() throws Exception {
		Stream<Record> result = database.select("count(employeeNumber) as count",
				"employees");
		assertEquals("[{count=19}]", "" + result.toList());
		result = database.select("max(budget) as maxBudget", "projects");
		assertEquals("[{maxBudget=3000000.0}]", "" + result.toList());

		result = database.select("sum(budget) as sumBudget", "projects");
		assertEquals("[{sumBudget=1.2E7}]", "" + result.toList());

		result = database.selectGroupBy("zipCode, count(employeeNumber) as employeeCount",
				"employees", "zipCode");
		List<String> l = new ArrayList<String>(
				result.toList().stream().map(e -> "" + e).toList());
		assertEquals(4, l.size());
		Collections.sort(l);
		assertEquals("{zipCode=12222, employeeCount=6}", "" + l.get(0));
		assertEquals("{zipCode=12225, employeeCount=3}", "" + l.get(3));

		result = database.selectGroupBy("budget, count(employeeNumber) as employeeCount",
				"employees natural join projects", "budget");
		l = new ArrayList<String>(result.toList().stream().map(e -> "" + e).toList());
		assertEquals(3, l.size());
		Collections.sort(l);
		assertEquals("{budget=1000000.0, employeeCount=6}", "" + l.get(0));
		assertEquals("{budget=3000000.0, employeeCount=7}", "" + l.get(2));
	}

}
