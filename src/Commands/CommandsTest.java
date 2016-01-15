package Commands;

import static org.junit.Assert.*;

import java.io.IOException;

import org.json.JSONException;
import org.junit.Test;

public class CommandsTest {

	@Test
	public void testCreateTableWithoutPK() {
		try {
			assertTrue(Commands.createTable("CREATE TABLE Person "
					+ "("
					+ "P_ID int NOT NULL,"
					+ "LastName varchar(255) NOT NULL,"
					+ "FirstName varchar(255),"
					+ "Address varchar(255)"
					+ ")"));
		} catch (IOException e) {
			e.printStackTrace();
			assertTrue(false);
		} catch (JSONException e) {
			e.printStackTrace();
			assertTrue(false);
		}
	}
	
	@Test
	public void testCreateTableWithOnePK() {
		try {
			assertTrue(Commands.createTable("CREATE TABLE Dudes "
					+ "("
					+ "P_Id int NOT NULL,"
					+ "LastName varchar(255) NOT NULL,"
					+ "FirstName varchar(255),"
					+ "Address varchar(255),"
					+ "Major varchar(30),"
					+ "PRIMARY KEY(P_Id)"
					+ ")"));
		} catch (IOException e) {
			e.printStackTrace();
			assertTrue(false);
		} catch (JSONException e) {
			e.printStackTrace();
			assertTrue(false);
		}
	}
	
	@Test
	public void testCreateTableWithMultiplePK () {
		try {
			assertTrue(Commands.createTable("CREATE TABLE Friends "
					+ "("
					+ "P_Id int NOT NULL,"
					+ "LastName varchar(255) NOT NULL,"
					+ "FirstName varchar(255),"
					+ "Address varchar(255),"
					+ "Year varchar(255),"
					+ "Major varchar(30),"
					+ "PRIMARY KEY(P_Id, LastName)"
					+ ")"));
		} catch (IOException e) {
			e.printStackTrace();
			assertTrue(false);
		} catch (JSONException e) {
			e.printStackTrace();
			assertTrue(false);
		}
	}
	
	@Test
	public void tesInsertTableWithPK() {
		try {
			assertTrue(Commands.insert("INSERT INTO Friends (P_Id, LastName, FirstName, Address, Year, Major)"
					+ "VALUES (3, Shah, Sahil 1326 Michillinda Ave., 2016, CMPE)"));
		} catch (IOException e) {
			e.printStackTrace();
			assertTrue(false);
		} catch (JSONException e) {
			e.printStackTrace();
			assertTrue(false);
		}
	}

}
