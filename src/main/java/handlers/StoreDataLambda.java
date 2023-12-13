package handlers;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;

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


public class StoreDataLambda implements RequestStreamHandler {
    private static final String database_url = "jdbc:postgresql://<private-aws-url-string>.us-east-2.rds.amazonaws.com:5432/postgres";
    @Override
    public void handleRequest(InputStream input, OutputStream output, Context context) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Connection connection = null;
        LambdaLogger LOGGER = context.getLogger();

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
            LOGGER.log("Getting DB credentials\n", LogLevel.DEBUG);
            try {
                getSecretValueResponse = client.getSecretValue(getSecretValueRequest);
            } catch (Exception e) {
                // For a list of exceptions thrown, see
                // https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
                LOGGER.log("ERROR" + e.getMessage(), LogLevel.ERROR);
                e.printStackTrace();
            }

            String secret = getSecretValueResponse.secretString();
            JsonNode secretJson = mapper.readTree(secret);
            String dbUsername = secretJson.get("username").asText();
            String dbPassword = secretJson.get("password").asText();
            LOGGER.log("Got DB credentials\n", LogLevel.DEBUG);
            LOGGER.log("Attempting to connect to the DB...\n", LogLevel.INFO);
            connection = DriverManager.getConnection(database_url, dbUsername, dbPassword);
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
            LOGGER.log("Error occurred while connecting to the database: \n" + e.getMessage(), LogLevel.ERROR);
            e.printStackTrace();

        } finally {
            try {
                if (connection != null && !connection.isClosed()) {
                    connection.close();
                }
            } catch (SQLException e) {
                LOGGER.log("Error occurred while closing the connection: \n" + e.getMessage(), LogLevel.ERROR);
                e.printStackTrace();
            }
        }
    }
}
