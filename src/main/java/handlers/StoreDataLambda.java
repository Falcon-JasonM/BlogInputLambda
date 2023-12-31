package handlers;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayV2HTTPResponse;
import com.amazonaws.services.lambda.runtime.logging.LogLevel;

import com.fasterxml.jackson.core.JsonProcessingException;
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
import java.util.HashMap;
import java.util.Map;

public class StoreDataLambda implements RequestStreamHandler {

    private static final String DATABASE_URL = System.getenv("DB_URL_KEY");

    @Override
    public void handleRequest(InputStream input, OutputStream output, Context context) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        Connection connection = null;
        LambdaLogger LOGGER = context.getLogger();
        JSONParser parser = new JSONParser();
        APIGatewayV2HTTPResponse responseEvent = new APIGatewayV2HTTPResponse();
        APIGatewayV2HTTPEvent requestEvent;
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        headers.put("Access-Control-Allow-Origin", "*");
        headers.put("Access-Control-Allow-Methods", "GET, PUT, PATCH, POST, OPTIONS");
        headers.put("Access-Control-Allow-Headers", "Content-Type");

        try {
                // Parse the received JSON content into an APIGatewayV2HTTPEvent object
                JSONObject event;
                try {
                    event = (JSONObject) parser.parse(new InputStreamReader(input));
                    LOGGER.log("Received event: " + event, LogLevel.DEBUG);
                } catch (IOException | ParseException ex) {
                    LOGGER.log("Error parsing input event into JSON format: " + ex.getMessage(), LogLevel.ERROR);
                    throw new RuntimeException(ex);
                }
                try {
                    requestEvent = mapper.readValue(event.toJSONString(), APIGatewayV2HTTPEvent.class);
                } catch (JsonProcessingException ex) {
                    LOGGER.log("Error parsing APIGatewayV2HTTPEvent: " + ex.getMessage(), LogLevel.ERROR);
                    throw new RuntimeException(ex);
                }
                LOGGER.log("Parsed APIGatewayV2HTTPEvent: " + requestEvent, LogLevel.DEBUG);
            // Return http status = 200 OK if request method is OPTIONS
            if ((requestEvent!= null && requestEvent.getHeaders().containsKey("httpMethod") &&
                    "OPTIONS".equals(requestEvent.getHeaders().get("httpMethod")))||requestEvent.getRouteKey().contains("OPTIONS")) {
                responseEvent.setStatusCode(200);
                responseEvent.setHeaders(headers);
                String responseBody = "{\"message\": \"OK\"}";
                responseEvent.setBody(responseBody);
                responseEvent.setIsBase64Encoded(false);
                LOGGER.log("Response to pre-flight check: " + responseEvent, LogLevel.DEBUG);
                try {
                    mapper.writeValue(output, responseEvent);
                } catch (IOException ex) {
                    LOGGER.log("Error processing pre-flight options request: " + ex.getMessage(), LogLevel.ERROR);
                    throw new RuntimeException(ex);
                }
                return;
            }

            if (requestEvent.getBody() != null) {
                JsonNode body = mapper.readTree(requestEvent.getBody());

                LOGGER.log("Body: " + body.toString(), LogLevel.INFO);

                String title = body.get("title").asText();
                String content = body.get("content").asText();

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

                responseEvent = new APIGatewayV2HTTPResponse();
                String responseBody = "{\"message\": \"Blog post successfully added\"}";
                responseEvent.setStatusCode(200);
                responseEvent.setBody(responseBody);
                responseEvent.setIsBase64Encoded(false);

                mapper.writeValue(output,responseEvent);
            } else {
                LOGGER.log("Request body is empty or missing", LogLevel.ERROR);
                responseEvent = new APIGatewayV2HTTPResponse();
                responseEvent.setStatusCode(400);
                responseEvent.setBody("\"Request body is empty or missing\"");
                responseEvent.setIsBase64Encoded(false);
                mapper.writeValue(output, responseEvent);
            }
        } catch (SQLException e) {
            LOGGER.log("Error occurred while connecting to the database: \n" + e.getMessage(), LogLevel.ERROR);
            e.printStackTrace();
        } catch (IOException e) {
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