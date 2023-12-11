import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest;
import com.amazonaws.services.secretsmanager.model.GetSecretValueResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class StoreDataLambda implements RequestStreamHandler {

    @Override
    public void handleRequest(InputStream input, OutputStream output, Context context) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Connection connection = null;

        try {
            AWSSecretsManager secretsManager = AWSSecretsManagerClientBuilder.defaultClient();
            String secretName = "prod/blogDB/dbUserPass";
            Region region = Region.of("us-east-2");
            private static final String DB_URL = "jdbc:postgresql:blog-post-db.cb61nkakvvkt.us-east-2.rds.amazonaws.com";
            GetSecretValueRequest getSecretValueRequest = new GetSecretValueRequest()
                    .withSecretId(secretName);
                    .withRegion(region.toString());
            GetSecretValueResult secretValueResult = secretsManager.getSecretValue(getSecretValueRequest);

            // Parse secret string to extract necessary values
            String secret = secretValueResult.getSecretString();
            JsonNode secretJson = mapper.readTree(secret);
            String dbUrl = DB_URL;
            String dbUsername = secretJson.get("username").asText();
            String dbPassword = secretJson.get("password").asText();

            connection = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
            JsonNode jsonInput = mapper.readTree(input);

            // Assuming JSON structure: { "title": "value1", "content": "value2" }
            String title = jsonInput.get("title").asText();
            String content = jsonInput.get("content").asText();

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
