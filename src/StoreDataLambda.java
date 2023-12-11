import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class StoreDataLambda implements RequestStreamHandler {

    private static final String DB_URL = "jdbc:postgresql:blog-post-db.cb61nkakvvkt.us-east-2.rds.amazonaws.com";
    private static final String DB_USERNAME = "postgres";
    private static final String DB_PASSWORD = "dfFVbYII5xeBzgEQ94sd";

    @Override
    public void handleRequest(InputStream input, OutputStream output, Context context) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Connection connection = null;

        try {
            connection = DriverManager.getConnection(DB_URL, DB_USERNAME, DB_PASSWORD);
            JsonNode jsonInput = mapper.readTree(input);

            // Assuming JSON structure: { "field1": "value1", "field2": "value2", "field3": "value3" }
            String field1 = jsonInput.get("title").asText();
            String field2 = jsonInput.get("content").asText();
            //String field3 = jsonInput.get("field3").asText();

            // Store data into PostgreSQL using prepared statement
            String sql = "INSERT INTO blog_page.blog_post (title, content) VALUES (?, ?)";
            PreparedStatement statement = connection.prepareStatement(sql);
            statement.setString(1, title);
            statement.setString(2, content);
            statement.executeUpdate();

            // Close resources
            statement.close();
            connection.close();
        } catch (SQLException e) {
            // Handle database exceptions
            e.printStackTrace();
        } finally {
            try {
                if (connection != null && !connection.isClosed()) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}