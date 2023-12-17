package handlers;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class StoreDataLambda implements RequestStreamHandler {
    private static final String DATABASE_URL = System.getenv("DB_URL_KEY");

    @Override
    public void handleRequest(InputStream input, OutputStream output, Context context) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Connection connection = null;
        LambdaLogger LOGGER = context.getLogger();
        JSONParser parser = new JSONParser();

        try {
            // Parse input to JSON
            JSONObject event = (JSONObject) parser.parse(new InputStreamReader(input));

            // Check to make sure the body was sent
            if (event.get("body") != null ) {
                JSONObject body = (JSONObject) parser.parse(event.get("body").toString());

                LOGGER.log("Body: " + body.toJSONString(), LogLevel.INFO);

                String title = body.get("title").toString();
                String content = body.get("content").toString();

                String secretName = System.getenv("SECRET_NAME");
                Region region = Region.of("us-east-2");

                SecretsManagerClient client = SecretsManagerClient.builder()
                        .region(region)
                        .build();

                GetSecretValueRequest getSecretValueRequest = GetSecretValueRequest.builder()
                        .secretId(secretName)
                        .build();

                GetSecretValueResponse getSecretValueResponse = client.getSecretValue(getSecretValueRequest);
                String secret = getSecretValueResponse.secretString();
                JsonNode secretJson = mapper.readTree(secret);
                String dbUsername = secretJson.get("username").asText();
                String dbPassword = secretJson.get("password").asText();

                LOGGER.log("Attempting to connect to the DB...\n", LogLevel.INFO);
                connection = DriverManager.getConnection(DATABASE_URL, dbUsername, dbPassword);

                String sql = "INSERT INTO blog_page.blog_post (title, content) VALUES (?, ?)";
                PreparedStatement statement = connection.prepareStatement(sql);
                statement.setString(1, title);
                statement.setString(2, content);
                statement.executeUpdate();
                statement.close();

                APIGatewayProxyResponseEvent responseEvent = new APIGatewayProxyResponseEvent();
                String responseBody = "{\"message\": \"Blog post successfully added\"}";
                responseEvent.setStatusCode(200);
                responseEvent.setBody(responseBody);
                responseEvent.setIsBase64Encoded(false);

                try (OutputStreamWriter writer = new OutputStreamWriter(output, "UTF-8")) {
                    writer.write(mapper.writeValueAsString(responseEvent));
                }
            } else {
                LOGGER.log("Request body is empty or missing", LogLevel.ERROR);
            }
        } catch (SQLException e) {
            LOGGER.log("Error occurred while connecting to the database: \n" + e.getMessage(), LogLevel.ERROR);
            e.printStackTrace();
        } catch (ParseException | IOException e) {
            LOGGER.log("Error occurred while parsing the request body: \n" + e.getMessage(), LogLevel.ERROR);
            throw new RuntimeException(e);
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