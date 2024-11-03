const dotenv = require('dotenv');
const axios = require('axios');
const gql = require('graphql-tag');
const { print } = require('graphql');
const { v4: uuidv4 } = require('uuid');
const { mqtt, iot } = require('aws-crt');
const { fromEnv } = require("@aws-sdk/credential-providers");

// Load environment variables
dotenv.config();

const {
    APP_SYNC_API_URL,
    APP_SYNC_API_KEY,
    NUMBER_OF_CONNECTIONS = '6',
    SQL_QUERY = 'SELECT * FROM job_data',
    CONNECTION_DURATION_MS = '60000',
    IOT_ENDPOINT,
    IOT_TOPIC,
    AWS_REGION = 'us-east-1'
} = process.env;

// Validate essential environment variables
if (!APP_SYNC_API_URL || !APP_SYNC_API_KEY || !IOT_ENDPOINT || !IOT_TOPIC) {
    console.error('Error: Missing required environment variables');
    process.exit(1);
}

const GET_DATA_QUERY = gql`
  query GetData($query: String!) {
    getData(query: $query) {
      transferId
    }
  }
`;

class ChunkReconstructor {
    constructor(transferId, onComplete) {
        this.transferId = transferId;
        this.chunks = new Map();
        this.totalChunks = null;
        this.onComplete = onComplete;
    }

    addChunk(metadata, data) {
        if (metadata.transfer_id !== this.transferId) return false;

        this.totalChunks = metadata.total_chunks;
        this.chunks.set(metadata.sequence_number, data);

        return this.isComplete();
    }

    isComplete() {
        if (!this.totalChunks) return false;

        for (let i = 1; i <= this.totalChunks; i++) {
            if (!this.chunks.has(i)) return false;
        }

        this.processCompleteData();
        return true;
    }

    processCompleteData() {
        const orderedData = Array.from(
            { length: this.totalChunks },
            (_, i) => Buffer.from(this.chunks.get(i + 1), 'base64')
        );

        const completeData = Buffer.concat(orderedData);
        this.onComplete(completeData);
    }
}

class IoTClient {
    constructor() {
        this.reconstructors = new Map();
        this.connection = null;
    }

    async connect() {
        try {
            // Get credentials from environment
            const credentialsProvider = fromEnv();
            const credentials = await credentialsProvider();

            // Create a new config builder with WebSocket
            const builder = iot.AwsIotMqttConnectionConfigBuilder
                .new_builder_for_websocket()
                .with_clean_session(true)
                .with_client_id(`test-client-${uuidv4()}`)
                .with_endpoint(IOT_ENDPOINT)
                .with_keep_alive_seconds(30)
                .with_ping_timeout_ms(3000)
                .with_protocol_operation_timeout_ms(60000)
                .with_credentials(
                    AWS_REGION,
                    credentials.accessKeyId,
                    credentials.secretAccessKey,
                    credentials.sessionToken
                );

            const config = builder.build();
            const client = new mqtt.MqttClient();
            this.connection = client.new_connection(config);

            return new Promise((resolve, reject) => {
                this.connection.on('connect', () => {
                    console.log('Successfully connected to AWS IoT');
                    resolve();
                });

                this.connection.on('error', (error) => {
                    console.error('Connection error:', error);
                    reject(error);
                });

                this.connection.on('disconnect', () => {
                    console.log('Disconnected from AWS IoT');
                });

                this.connection.on('message', (topic, payload) => {
                    this.handleMessage(topic, payload);
                });

                this.connection.connect();
            });
        } catch (err) {
            console.error('Error setting up MQTT client:', err);
            throw err;
        }
    }

    async subscribe(transferId, onComplete) {
        if (!this.connection) {
            throw new Error('Client not connected');
        }

        this.reconstructors.set(transferId, new ChunkReconstructor(transferId, onComplete));

        try {
            await this.connection.subscribe(
                IOT_TOPIC,
                mqtt.QoS.AtLeastOnce
            );
            console.log(`Subscribed to topic: ${IOT_TOPIC}`);
        } catch (err) {
            console.error('Error subscribing to topic:', err);
            throw err;
        }
    }

    handleMessage(topic, payloadBuffer) {
        try {
            // Convert ArrayBuffer to Buffer if needed
            const buffer = Buffer.from(payloadBuffer);

            // Try to parse as JSON first
            try {
                const message = JSON.parse(buffer.toString());
                const { metadata, data } = message;

                const reconstructor = this.reconstructors.get(metadata.transfer_id);
                if (!reconstructor) {
                    console.log(`No reconstructor found for transfer ${metadata.transfer_id}`);
                    return;
                }

                if (reconstructor.addChunk(metadata, data)) {
                    this.reconstructors.delete(metadata.transfer_id);
                }
            } catch (jsonError) {
                // If JSON parsing fails, treat it as binary data
                console.log('Received binary message, processing as Arrow data...');

                // You might want to add additional logic here to determine
                // which transfer ID this binary data belongs to

                // For now, let's log the binary data length
                console.log(`Received binary data of length: ${buffer.length}`);
            }
        } catch (error) {
            console.error('Error processing message:', error);
        }
    }

    async disconnect() {
        if (this.connection) {
            await this.connection.disconnect();
            this.connection = null;
        }
    }
}

async function performGraphQLQuery(iotClient, query, queryId) {
    try {
        const response = await axios.post(
            APP_SYNC_API_URL,
            {
                query: print(GET_DATA_QUERY),
                variables: { query },
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

        await iotClient.subscribe(transferId, (completeData) => {
            console.log(`Received complete data for transfer ${transferId}`);
            // Process your data here
            // Example: console.log(completeData.toString());
        });

    } catch (error) {
        console.error(`Query ${queryId} Failed:`, error.message);
    }
}

async function startConcurrentQueries(numberOfConnections, sqlQuery, durationMs) {
    console.log(`Starting ${numberOfConnections} concurrent GraphQL queries for ${durationMs} ms...`);

    const iotClient = new IoTClient();
    await iotClient.connect();

    const startTime = Date.now();
    const endTime = startTime + durationMs;
    const connectionIds = Array.from({ length: numberOfConnections }, () => uuidv4());

    const queryLoop = async (connectionId) => {
        while (Date.now() < endTime) {
            await performGraphQLQuery(iotClient, sqlQuery, connectionId);
            await new Promise(resolve => setTimeout(resolve, 1000));
        }
        console.log(`Connection ${connectionId} finished.`);
    };

    try {
        await Promise.all(connectionIds.map(id => queryLoop(id)));
    } finally {
        await iotClient.disconnect();
    }

    console.log('All connections have completed their queries.');
}

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
        process.exit(1);
    }

    console.log('--- Testing Completed ---');
})();