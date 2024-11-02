#!/usr/bin/env node

/**
 * test-sdk.js
 *
 * A CLI script to test the Lambda function via the AppSync GraphQL API.
 * It performs multiple concurrent GraphQL queries based on the provided configuration.
 * It expects a 'transferId' in the response to verify data publication to AWS IoT.
 */

const dotenv = require('dotenv');
const axios = require('axios');
const gql = require('graphql-tag'); // Correct import for gql
const { print } = require('graphql'); // Correct import for print
const { v4: uuidv4 } = require('uuid');

// Load environment variables from .env file
dotenv.config();

// Destructure environment variables with defaults where applicable
const {
    APP_SYNC_API_URL,
    APP_SYNC_API_KEY,
    NUMBER_OF_CONNECTIONS = 6,
    SQL_QUERY = 'SELECT * FROM job_data',
    CONNECTION_DURATION_MS = 60000,
    IOT_ENDPOINT,
    IOT_TOPIC,
    AWS_REGION,
    COGNITO_IDENTITY_POOL_ID,
} = process.env;

// Validate essential environment variables
if (!APP_SYNC_API_URL || !APP_SYNC_API_KEY) {
    console.error('Error: APP_SYNC_API_URL and APP_SYNC_API_KEY must be set in the .env file.');
    process.exit(1);
}

// GraphQL Query Definition expecting 'transferId'
const GET_DATA_QUERY = gql`
  query GetData($query: String!) {
    getData(query: $query) {
      transferId
    }
  }
`;

// Function to perform a single GraphQL query
const performGraphQLQuery = async (query, queryId) => {
    try {
        const response = await axios.post(
            APP_SYNC_API_URL,
            {
                query: print(GET_DATA_QUERY), // Serialize the AST to a string
                variables: {
                    query,
                },
            },
            {
                headers: {
                    'Content-Type': 'application/json',
                    'x-api-key': APP_SYNC_API_KEY,
                },
            }
        );

        if (response.data.errors) {
            console.error(`Query ${queryId} Errors:`, JSON.stringify(response.data.errors, null, 2));
            return;
        }

        const transferId = response.data.data.getData.transferId;
        console.log(`Query ${queryId} Success: Received Transfer ID - ${transferId}`);

        // Optional: Implement logic to use transferId, such as listening to AWS IoT
        // For example, you can add code here to verify data publication using transferId
    } catch (error) {
        console.error(`Query ${queryId} Failed:`, error.message);
    }
};

// Function to start multiple concurrent queries
const startConcurrentQueries = async (numberOfConnections, sqlQuery, durationMs) => {
    console.log(`Starting ${numberOfConnections} concurrent GraphQL queries for ${durationMs} ms...`);

    const startTime = Date.now();
    const endTime = startTime + durationMs;

    // Generate unique IDs for each connection
    const connectionIds = Array.from({ length: numberOfConnections }, () => uuidv4());

    // Function to continuously perform queries until the duration expires
    const queryLoop = async (connectionId) => {
        while (Date.now() < endTime) {
            await performGraphQLQuery(sqlQuery, connectionId);
            // Optionally, add a short delay between queries to avoid overwhelming the API
            await new Promise((resolve) => setTimeout(resolve, 1000)); // 1-second delay
        }
        console.log(`Connection ${connectionId} finished.`);
    };

    // Start all query loops concurrently
    await Promise.all(connectionIds.map((id) => queryLoop(id)));

    console.log('All connections have completed their queries.');
};

// Main Execution
(async () => {
    console.log('--- Lambda Function Tester ---');
    console.log(`AppSync API URL: ${APP_SYNC_API_URL}`);
    console.log(`Number of Connections: ${NUMBER_OF_CONNECTIONS}`);
    console.log(`SQL Query: ${SQL_QUERY}`);
    console.log(`Connection Duration: ${CONNECTION_DURATION_MS} ms\n`);

    try {
        await startConcurrentQueries(
            parseInt(NUMBER_OF_CONNECTIONS, 10),
            SQL_QUERY,
            parseInt(CONNECTION_DURATION_MS, 10)
        );
    } catch (error) {
        console.error('An unexpected error occurred:', error);
    }

    console.log('--- Testing Completed ---');
})();
