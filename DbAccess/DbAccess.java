import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Database connection test
 *
 * @author Frank Beyer
 * @version 2018_01_31 1.0.0
 */
public class DbAccess {
    // init database constants
    private static final String DATABASE_DRIVER = "com.mysql.jdbc.Driver";
    private static final String DATABASE_URL = "jdbc:mysql://localhost:3306/fb_test";
    private static final String USERNAME = "Tester";
    private static final String PASSWORD = "tester";

    // init connection object
    private Connection myConn;
    // init properties object
    private Properties myProp;

    // create properties
    private Properties getProperties() {
      if (myProp == null) {
        myProp = new Properties();
        myProp.setProperty("user", USERNAME);
        myProp.setProperty("password", PASSWORD);
      } return myProp;
    }

  /**
   * Constructor for objects of class DbAccess
   */
  public DbAccess() {
    System.out.println("Connecting database...");
    myConn = dbConnect();
    if (myConn != null) {
      System.out.println("Database connected!");
      dbDisconnect();
    }
 
  }

  // connect database
  public Connection dbConnect() {
    if (myConn == null) {
      try {
        Class.forName(DATABASE_DRIVER);
        myConn = DriverManager.getConnection(DATABASE_URL, getProperties());
      } catch (ClassNotFoundException | SQLException e) {
        e.printStackTrace();
      }
    } return myConn;
  }

  // disconnect database
  public void dbDisconnect() {
    if (myConn != null) {
      try { 
        myConn.close(); myConn = null; 
        System.out.println("Database disconnected!");
      } catch (SQLException e) { e.printStackTrace(); }
    }
  }

}
