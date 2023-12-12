package handlers;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;

// Make sure to import the following packages in your code
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.logging.Logger;

public class StoreDataLambda implements RequestStreamHandler {

    public static final Logger LOGGER = Logger.getLogger(StoreDataLambda.class.getName());
    private static final String DB_URL = "jdbc:postgresql:blog-post-db.cb61nkakvvkt.us-east-2.rds.amazonaws.com:5432/postgres";
    @Override
    public void handleRequest(InputStream input, OutputStream output, Context context) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Connection connection = null;

        try {
            String secretName = "prod/blogDB/dbUserPass";
            Region region = Region.of("us-east-2");

            // Create a Secrets Manager client
            SecretsManagerClient client = SecretsManagerClient.builder()
                    .region(region)
                    .build();

            GetSecretValueRequest getSecretValueRequest = GetSecretValueRequest.builder()
                    .secretId(secretName)
                    .build();

            GetSecretValueResponse getSecretValueResponse = null;

            try {
                getSecretValueResponse = client.getSecretValue(getSecretValueRequest);
            } catch (Exception e) {
                // For a list of exceptions thrown, see
                // https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
                LOGGER.severe("Error occurred while retrieving secret value: " + e.getMessage());
                e.printStackTrace();
            }

            String secret = getSecretValueResponse.secretString();
            JsonNode secretJson = mapper.readTree(secret);
            String dbUrl = DB_URL;
            String dbUsername = secretJson.get("username").asText();
            String dbPassword = secretJson.get("password").asText();
            LOGGER.info("Attempting to connect to the DB...");
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
            LOGGER.severe("Error occurred while connecting to the database: " + e.getMessage());
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
