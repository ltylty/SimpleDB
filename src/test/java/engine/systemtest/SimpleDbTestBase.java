package engine.systemtest;

import org.junit.Before;

import engine.Database;

/**
 * Base class for all SimpleDb test classes. 
 * @author nizam
 *
 */
public class SimpleDbTestBase {
	/**
	 * Reset the database before each test is run.
	 */
	@Before	public void setUp() throws Exception {					
		Database.reset();
	}
	
}
