public class Config {
    //public static String url = "jdbc:mysql://localhost:3306/mysql?useLegacyDatetimeCode=false&serverTimezone=UTC"; if run in local
    //public static String url = "jdbc:mysql://host.docker.internal:3310/mysql?useLegacyDatetimeCode=false&serverTimezone=UTC"; // if run in docker
    public static String url = "jdbc:mysql://172.17.0.1:3310/mysql?useLegacyDatetimeCode=false&serverTimezone=UTC"; //if run in aws dockers
    public static String driver = "com.mysql.jdbc.Driver";
    public static String username = "root";
    // public static String password = "Dima1790426";
    public static String password = "";
   // public static String data_path = "C:\\Users\\DmitryMakarevich\\Desktop\\homeworks\\6_Java\\fund";
  public static String data_path = "/homeworks/6_Java/fund";
}
