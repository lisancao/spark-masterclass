// Chapter 15: query a Spark Connect server through the Connect JDBC driver (new in Spark 4.1).
// The driver is in the tarball, in jars/connect-repl/. See ch15/connect_jdbc.sh.
import java.sql.*;

public class ConnectJdbc {
  public static void main(String[] args) throws Exception {
    String url = args.length > 0 ? args[0] : "jdbc:sc://localhost:15102";
    try (Connection c = DriverManager.getConnection(url)) {
      DatabaseMetaData md = c.getMetaData();
      System.out.println("driver:  " + md.getDriverName() + " " + md.getDriverVersion());
      System.out.println("product: " + md.getDatabaseProductName() + " " + md.getDatabaseProductVersion());
      try (Statement s = c.createStatement();
           ResultSet r = s.executeQuery(
             "SELECT current_catalog() AS cat, typeof(array(1, 2)) AS t, array(1, 2) AS a, " +
             "named_struct('x', 1) AS st")) {
        ResultSetMetaData m = r.getMetaData();
        for (int i = 1; i <= m.getColumnCount(); i++)
          System.out.printf("column %d: %-4s %-10s %s%n", i, m.getColumnName(i),
                            m.getColumnTypeName(i), JDBCType.valueOf(m.getColumnType(i)));
        while (r.next())
          System.out.println("row:     " + r.getString(1) + " | " + r.getString(2) + " | "
                             + r.getObject(3) + " | " + r.getObject(4));
      }
      try (PreparedStatement p = c.prepareStatement(
             "SELECT id, id * ? AS scaled FROM range(3)")) {
        p.setInt(1, 10);
        try (ResultSet r = p.executeQuery()) {
          while (r.next()) System.out.println("prepared: " + r.getLong(1) + " -> " + r.getLong(2));
        }
      } catch (SQLException e) {
        System.out.println("prepareStatement: " + e.getClass().getSimpleName()
                           + (e.getMessage() == null ? "" : ": " + e.getMessage().split("\n")[0]));
      }
    }
  }
}
