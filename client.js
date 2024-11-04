const dotenv = require('dotenv');
const axios = require('axios');
const gql = require('graphql-tag');
const { print } = require('graphql');
const { v4: uuidv4 } = require('uuid');
const { mqtt, iot } = require('aws-crt');
const { fromCognitoIdentityPool } = require("@aws-sdk/credential-providers");
const { RecordBatchStreamReader } = require('apache-arrow');
const { CognitoIdentityClient } = require("@aws-sdk/client-cognito-identity");

dotenv.config();

const {
    APP_SYNC_API_URL,
    APP_SYNC_API_KEY,
    IOT_ENDPOINT,
    IOT_TOPIC,
    COGNITO_IDENTITY_POOL_ID,
    AWS_REGION = 'us-east-1'
} = process.env;

const EXECUTE_QUERY = gql`
    query ExecuteQuery($query: String!) {
        getData(query: $query) {
            transferId
            metadata {
                rowCount
                chunkCount
                schema
            }
        }
    }
`;

class ChunkManager {
    constructor(transferId, onComplete) {
        this.transferId = transferId;
        this.chunks = new Map();
        this.onComplete = onComplete;
        this.receivedSequences = new Set();
        console.log(`ChunkManager created for transferId: ${transferId}`);
    }

    addChunk(message) {
        console.log(`Adding chunk for transferId: ${this.transferId}`);
        try {
            const { metadata, data } = message;

            if (metadata.transfer_id !== this.transferId) {
                console.warn(`Transfer ID mismatch. Expected: ${this.transferId}, Received: ${metadata.transfer_id}`);
                return false;
            }

            if (this.receivedSequences.has(metadata.sequence_number)) {
                console.warn(`Duplicate chunk received for sequence ${metadata.sequence_number} in transfer ${this.transferId}`);
                return false;
            }

            // Decode base64 data
            const chunkBuffer = Buffer.from(data, 'base64');
            this.chunks.set(metadata.sequence_number, chunkBuffer);
            this.receivedSequences.add(metadata.sequence_number);
            console.log(`Chunk ${metadata.sequence_number}/${metadata.total_chunks} added for transfer ${this.transferId}`);

            // Check if we have all chunks
            if (metadata.total_chunks > 0 &&
                this.receivedSequences.size === metadata.total_chunks) {
                console.log(`All chunks received for transfer ${this.transferId}. Processing complete data.`);
                this.processCompleteData();
                return true;
            }

            return false;
        } catch (error) {
            console.error('Error processing chunk:', error);
            return false;
        }
    }

    async processCompleteData() {
        console.log(`Processing complete data for transfer ${this.transferId}`);
        try {
            // Sort chunks by sequence number
            const orderedChunks = Array.from(this.chunks.entries())
                .sort(([a], [b]) => a - b)
                .map(([_, chunk]) => chunk);

            const completeBuffer = Buffer.concat(orderedChunks);
            console.log(`Complete data size for transfer ${this.transferId}: ${completeBuffer.length} bytes`);

            // Process Arrow format data
            const reader = await RecordBatchStreamReader.from(completeBuffer);
            const batches = await reader.readAll();
            console.log(`Arrow RecordBatchStreamReader read ${batches.length} batches for transfer ${this.transferId}`);

            // Convert batches to regular JavaScript objects
            const allRecords = batches.flatMap(batch => batch.toArray());
            console.log(`Total records processed for transfer ${this.transferId}: ${allRecords.length}`);

            this.onComplete(allRecords, null);
            this.cleanup();
        } catch (error) {
            console.error('Error processing complete data:', error);
            this.onComplete(null, error);
            this.cleanup();
        }
    }

    cleanup() {
        console.log(`Cleaning up ChunkManager for transfer ${this.transferId}`);
        this.chunks.clear();
        this.receivedSequences.clear();
    }
}

class IoTClient {
    constructor() {
        this.client = new mqtt.MqttClient();
        this.connection = null;
        this.chunkManagers = new Map();
        this.isSubscribed = false;
        console.log("IoTClient instance created.");
    }

    async connect() {
        try {
            console.log('Initializing Cognito credentials...');
            const cognitoClient = new CognitoIdentityClient({ region: AWS_REGION });
            const credentialsProvider = fromCognitoIdentityPool({
                client: cognitoClient,
                identityPoolId: COGNITO_IDENTITY_POOL_ID,
            });

            console.log('Obtaining credentials...');
            const credentials = await credentialsProvider();
            console.log('Credentials obtained successfully');

            const clientId = `test-client-${uuidv4()}`;
            console.log(`Creating MQTT client with ID: ${clientId}`);

            const builder = iot.AwsIotMqttConnectionConfigBuilder
                .new_builder_for_websocket()
                .with_clean_session(true)
                .with_client_id(clientId)
                .with_endpoint(IOT_ENDPOINT)
                .with_credentials(
                    AWS_REGION,
                    credentials.accessKeyId,
                    credentials.secretAccessKey,
                    credentials.sessionToken
                )
                .with_keep_alive_seconds(60);

            const config = builder.build();
            console.log('MQTT connection config built');

            this.connection = this.client.new_connection(config);

            return new Promise((resolve, reject) => {
                let connectTimeout = setTimeout(() => {
                    reject(new Error('Connection timeout after 15 seconds'));
                }, 15000);

                this.connection.on('connect', () => {
                    clearTimeout(connectTimeout);
                    console.log('Successfully connected to AWS IoT');
                    resolve();
                });

                this.connection.on('error', (error) => {
                    clearTimeout(connectTimeout);
                    console.error('Connection error:', error);
                    reject(error);
                });

                this.connection.on('disconnect', () => {
                    console.log('Disconnected from AWS IoT');
                    this.isSubscribed = false;
                });

                this.connection.on('message', (topic, payload) => {
                    console.log(`Message received on topic: ${topic}`);
                    this.handleMessage(topic, payload);
                });

                this.connection.on('connection_failure', (error) => {
                    console.error('Connection failure:', error);
                });

                this.connection.on('connection_success', () => {
                    console.log('Connection successful');
                });

                console.log('Initiating connection...');
                this.connection.connect();
            });
        } catch (error) {
            console.error('Failed to connect:', error);
            throw error;
        }
    }

    async subscribe(transferId, onComplete) {
        if (!this.connection) {
            throw new Error('Client not connected');
        }

        console.log(`Preparing to subscribe to topic ${IOT_TOPIC} for transferId ${transferId}`);
        const chunkManager = new ChunkManager(transferId, onComplete);
        this.chunkManagers.set(transferId, chunkManager);

        return new Promise((resolve, reject) => {
            // Increase subscription timeout to 15 seconds
            let subscribeTimeout = setTimeout(() => {
                reject(new Error('Subscription timeout after 15 seconds'));
            }, 15000);

            console.log(`Attempting to subscribe to ${IOT_TOPIC}...`);
            this.connection.subscribe(
                IOT_TOPIC,
                mqtt.QoS.AtLeastOnce,
                (error, response) => {
                    clearTimeout(subscribeTimeout);

                    if (error) {
                        console.error('Subscription error:', error);
                        reject(error);
                        return;
                    }

                    if (!response) {
                        console.error('No response received from subscription attempt');
                        reject(new Error('No subscription response received'));
                        return;
                    }

                    console.log('Subscription response:', response);
                    this.isSubscribed = true;
                    resolve();
                }
            );
        });
    }

    handleMessage(topic, payloadBuffer) {
        console.log(`Message received on topic ${topic}, payload size: ${payloadBuffer.length} bytes`);
        try {
            const message = JSON.parse(payloadBuffer.toString());
            console.log('Message type:', message.type);

            if (message.type !== 'arrow_data') {
                console.log('Received non-arrow data message:', message.type);
                return;
            }

            const transferId = message.metadata.transfer_id;
            console.log(`Processing message for transfer ID: ${transferId}`);

            const chunkManager = this.chunkManagers.get(transferId);
            if (!chunkManager) {
                console.warn(`No ChunkManager found for transferId ${transferId}`);
                return;
            }

            const isComplete = chunkManager.addChunk(message);
            if (isComplete) {
                console.log(`Transfer ${transferId} is complete`);
                this.chunkManagers.delete(transferId);
            }
        } catch (error) {
            console.error('Error handling message:', error);
            console.error('Raw payload:', payloadBuffer.toString());
        }
    }

    async executeQuery(query) {
        console.log(`Executing query: ${query}`);
        try {
            await this.connect();

            console.log('Sending GraphQL query to AppSync API...');
            const response = await axios.post(
                APP_SYNC_API_URL,
                {
                    query: print(EXECUTE_QUERY),
                    variables: { query }
                },
                {
                    headers: {
                        'Content-Type': 'application/json',
                        'x-api-key': APP_SYNC_API_KEY
                    }
                }
            );

            if (response.data.errors) {
                throw new Error(`GraphQL errors: ${JSON.stringify(response.data.errors)}`);
            }

            const { getData } = response.data.data;
            console.log('Query accepted:', getData);

            // Subscribe before setting up the data promise
            console.log('Setting up subscription...');
            await this.subscribe(getData.transferId, (data, error) => {
                if (error) console.error('Chunk processing error:', error);
                else console.log('Chunk processed successfully');
            });

            const dataPromise = new Promise((resolve, reject) => {
                const timeout = setTimeout(() => {
                    reject(new Error('Data reception timed out after 60 seconds'));
                }, 60000);

                // Set up message handler
                const messageHandler = (topic, payload) => {
                    console.log('Message received in promise handler');
                    try {
                        const message = JSON.parse(payload.toString());
                        if (message.metadata.transfer_id === getData.transferId) {
                            clearTimeout(timeout);
                            this.connection.removeListener('message', messageHandler);
                            resolve(message);
                        }
                    } catch (error) {
                        console.error('Error processing message in promise:', error);
                    }
                };

                this.connection.on('message', messageHandler);
            });

            console.log('Waiting for data via MQTT...');
            const results = await dataPromise;

            await this.disconnect();
            return results;

        } catch (error) {
            console.error('Query execution failed:', error);
            await this.disconnect();
            throw error;
        }
    }

    async disconnect() {
        if (this.connection) {
            console.log('Disconnecting from AWS IoT...');
            try {
                await this.connection.disconnect();
                this.connection = null;
                this.isSubscribed = false;
                console.log('Successfully disconnected from AWS IoT');
            } catch (error) {
                console.error('Error during disconnect:', error);
            }
        }
    }
}

async function executeQuery(query) {
    console.log(`Executing query: ${query}`);
    try {
        // Initialize IoT client
        const iotClient = new IoTClient();
        await iotClient.connect();

        console.log('Sending GraphQL query to AppSync API.');

        // Execute GraphQL query
        const response = await axios.post(
            APP_SYNC_API_URL,
            {
                query: print(EXECUTE_QUERY),
                variables: { query }
            },
            {
                headers: {
                    'Content-Type': 'application/json',
                    'x-api-key': APP_SYNC_API_KEY
                }
            }
        );
        console.log('GraphQL query sent. Awaiting response...');

        // Add detailed error logging
        if (response.data.errors) {
            console.error('GraphQL errors:', response.data.errors);
            throw new Error(response.data.errors[0].message);
        }

        if (!response.data || !response.data.data) {
            console.error('Unexpected response structure:', response.data);
            throw new Error('Invalid response from API');
        }

        const { getData } = response.data.data;

        console.log('Query accepted by AppSync:', {
            transferId: getData.transferId,
            rowCount: getData.metadata.rowCount,
            chunkCount: getData.metadata.chunkCount
        });

        // Set up promise for data reception
        const dataPromise = new Promise((resolve, reject) => {
            // Set a timeout (e.g., 30 seconds)
            const timeout = setTimeout(() => {
                reject(new Error('Data reception timed out.'));
            }, 30000);

            iotClient.subscribe(getData.transferId, (data, error) => {
                clearTimeout(timeout);
                if (error) reject(error);
                else resolve(data);
            });
        });


// Wait for all data to be received
        console.log('Waiting for data to be received via MQTT...');
        const results = await dataPromise;
        console.log('Data received successfully via MQTT.');


        // Cleanup
        await iotClient.disconnect();

        return {
            transferId: getData.transferId,
            totalRows: getData.metadata.rowCount,
            data: results,
            schema: getData.metadata.schema
        };
    } catch (error) {
        console.error('Query execution failed:', error);
        if (error.response) {
            console.error('Response data:', error.response.data);
            console.error('Response status:', error.response.status);
        }
        throw error;
    }
}

// Main execution
async function main() {
    const iotClient = new IoTClient();
    try {
        const query = 'SELECT * FROM job_data LIMIT 100';
        console.log('Executing query:', query);
        const results = await iotClient.executeQuery(query);
        console.log('Query execution completed:', results);
    } catch (error) {
        console.error('Error in main:', error);
        process.exit(1);
    }
}

if (require.main === module) {
    main();
}

module.exports = { IoTClient };