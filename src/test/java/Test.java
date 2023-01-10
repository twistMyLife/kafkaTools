import org.junit.Before;

import java.sql.*;

public class Test {
    Connection connection;
    Statement statement;

    @Before
    public void before(){
        try {
            //        Class.forName("org.sqlite.JDBC");
             connection = DriverManager.getConnection("jdbc:sqlite:C:\\Users\\CETC10\\Desktop\\gqw\\kafkaTools\\db\\kafkaTools.db");
             statement = connection.createStatement();
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    @org.junit.Test
    public void insert() throws SQLException {
        String insert = "INSERT INTO ConnectionInfo (ID, NAME, AGE)\n" +
                "VALUES" +
                "(1, 'GQW', 26);";
        statement.executeUpdate(insert);
    }

    @org.junit.Test
    public void creatProducerTable() throws SQLException {
        String creatTable = "CREATE TABLE producer_info (" +
                "id int(20) PRIMARY KEY NOT NULL," +
                "record_connection_info_id int(20)  NOT NULL," +
                "producer_name VARCHAR(255) NOT NULL," +
                "producer_topic VARCHAR(255) NOT NULL," +
                "comment VARCHAR(255) ," +
                "last_content MESSAGE_TEXT," +
                "is_cycle_send int(20) ," +
                "is_random_append int(20) ," +
                "send_interval int(20)" +
                ")";
        statement.executeUpdate(creatTable);
    }

    @org.junit.Test
    public void creatConsumerTable() throws SQLException {
        String creatTable = "CREATE TABLE consumer_info (" +
                "id int(20) PRIMARY KEY NOT NULL," +
                "record_connection_info_id int(20)  NOT NULL," +
                "consumer_name VARCHAR(255) NOT NULL," +
                "consumer_topic VARCHAR(255) NOT NULL," +
                "consumer_group_name VARCHAR(255) NOT NULL," +
                "auto_commit int(20) NOT NULL," +
                "status int(20)" +
                ")";
        statement.executeUpdate(creatTable);
    }

    @org.junit.Test
    public void creatConnectionTable() throws SQLException {
        String creatTable = "CREATE TABLE connection_info (" +
                "id int(20) PRIMARY KEY NOT NULL," +
                "kafka_name VARCHAR(255) NOT NULL," +
                "kafka_version VARCHAR(255) ," +
                "kafka_bootstrap VARCHAR(255) NOT NULL ," +
                "zookeeper_bootstrap VARCHAR(255)" +
                ")";
        statement.executeUpdate(creatTable);
    }

    @org.junit.Test
    public void creatTopicTable() throws SQLException {
        String creatTable = "CREATE TABLE topic_info (" +
                "id int(20) PRIMARY KEY NOT NULL," +
                "record_connection_info_id int(20)  NOT NULL," +
                "topic_name VARCHAR(255) NOT NULL," +
                "partitions int(20) NOT NULL," +
                "replicas int(20) NOT NULL "+
                ")";
        statement.executeUpdate(creatTable);
    }



    @org.junit.Test
    public void IdAuto() throws SQLException {
        String IdAuto = "ALTER TABLE connection_info ADD id INT identity (1, 1)";
        statement.executeUpdate(IdAuto);
    }


    @org.junit.Test
    public void select() throws SQLException {
        String select = "SELECT * FROM COMPANY;";
        ResultSet resultSet = statement.executeQuery(select);
        while (resultSet.next()){
            int id = resultSet.getInt("id");
            String name = resultSet.getString("name");
            int age = resultSet.getInt("age");
            System.out.println();
        }
    }

    @org.junit.Test
    public void selectMaxId() throws SQLException {
        String select = "SELECT * FROM  connection_info ;";
        ResultSet resultSet = statement.executeQuery(select);
        while (resultSet.next()){
            int id = resultSet.getInt("id");
            String name = resultSet.getString("kafka_name");
            System.out.println(id +"   " +name);
        }
    }
}
