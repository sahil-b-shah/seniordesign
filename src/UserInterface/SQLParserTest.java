package UserInterface;

import static org.junit.Assert.*;

import java.io.IOException;
import java.sql.SQLException;

import org.json.JSONException;
import org.junit.BeforeClass;
import org.junit.Test;

public class SQLParserTest {
	
	private static SQLParser parser;

    @BeforeClass
    public static void setUp() { 
    	parser = new SQLParser(true);
    }
	
	@Test
	public void testCreateTableValid() {
		try {
			assertTrue(parser.parse("CREATE TABLE blah blah blah"));
		} catch (SQLException e) {
			assertTrue(false);
		} catch (IOException e) {
			assertTrue(false);
			e.printStackTrace();
		} catch (JSONException e) {
			assertTrue(false);
		}
	}

	@Test
	public void testInsertValid() {
		try {
			assertTrue(parser.parse("INSERT INTO blah blah blah VALUES blah blah blah"));
			assertTrue(parser.parse("INSERT INTO blah VALUES blah"));
		} catch (SQLException e) {
			assertTrue(false);
		} catch (IOException e) {
			assertTrue(false);
		} catch (JSONException e) {
			assertTrue(false);
		}
	}
	
	public void testInsertInvalid(){
		try {
			assertFalse(parser.parse("INSERT INTO VALUES blah blah blah"));
			assertFalse(parser.parse("INSERT INTO blah blah blah VALUES"));
			assertFalse(parser.parse("INSERT INTO VALUES"));
			assertFalse(parser.parse("INSERT INTO blah blah"));
			assertFalse(parser.parse("INSERT blah blah VALUES blah blah"));
		} catch (SQLException e) {
			assertTrue(false);
		} catch (IOException e) {
			assertTrue(false);
		} catch (JSONException e) {
			assertTrue(false);
		}
	}
}
