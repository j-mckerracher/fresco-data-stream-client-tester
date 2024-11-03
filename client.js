const dotenv = require('dotenv');
const axios = require('axios');
const gql = require('graphql-tag');
const { print } = require('graphql');
const { v4: uuidv4 } = require('uuid');
const { mqtt, iot } = require('aws-crt');
const { fromEnv } = require("@aws-sdk/credential-providers");
const crypto = require('crypto');
const fs = require('fs'); // For optional data saving
const { RecordBatchStreamReader } = require('apache-arrow'); // Correct import

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
            metadata {
                rowCount
                chunkCount
                schema
            }
        }
    }
`;

class ChunkReconstructor {
    constructor(transferId, onComplete) {
        this.transferId = transferId;
        this.chunks = new Map();
        this.totalChunks = null;
        this.onComplete = onComplete;
        this.receivedSequences = new Set();
        this.timeoutId = null;
        this.retryCount = new Map();
        this.MAX_RETRIES = 3;
        this.CHUNK_TIMEOUT = 5000;
    }

    addChunk(message) {
        try {
            const { metadata, data } = message;

            if (metadata.transfer_id !== this.transferId) {
                return false;
            }

            if (this.receivedSequences.has(metadata.sequence_number)) {
                console.log(`Duplicate chunk received for sequence ${metadata.sequence_number}`);
                return false;
            }

            if (!this.totalChunks) {
                this.totalChunks = metadata.total_chunks;
                this.startChunkTimeout();
            }

            // Verify checksum
            const receivedChecksum = metadata.checksum;
            const chunkBuffer = Buffer.from(data, 'base64');
            const calculatedChecksum = this.createChecksum(chunkBuffer);

            if (receivedChecksum !== calculatedChecksum) {
                console.error(`Checksum mismatch for chunk ${metadata.sequence_number}`);
                this.requestChunkRetry(metadata.sequence_number);
                return false;
            }

            this.chunks.set(metadata.sequence_number, chunkBuffer);
            this.receivedSequences.add(metadata.sequence_number);

            if (this.isComplete()) {
                this.clearChunkTimeout();
                this.processCompleteData();
                return true;
            }

            this.restartChunkTimeout();
            return false;
        } catch (error) {
            console.error('Error processing chunk:', error);
            return false;
        }
    }

    createChecksum(buffer) {
        return crypto.createHash('md5').update(buffer).digest('hex');
    }

    isComplete() {
        if (!this.totalChunks) return false;
        return this.receivedSequences.size === this.totalChunks;
    }

    startChunkTimeout() {
        this.timeoutId = setTimeout(() => {
            this.handleMissingChunks();
        }, this.CHUNK_TIMEOUT);
    }

    clearChunkTimeout() {
        if (this.timeoutId) {
            clearTimeout(this.timeoutId);
            this.timeoutId = null;
        }
    }

    restartChunkTimeout() {
        this.clearChunkTimeout();
        this.startChunkTimeout();
    }

    handleMissingChunks() {
        if (!this.totalChunks) return;

        const missingChunks = [];
        for (let i = 1; i <= this.totalChunks; i++) {
            if (!this.receivedSequences.has(i)) {
                missingChunks.push(i);
            }
        }

        if (missingChunks.length > 0) {
            console.log(`Missing chunks: ${missingChunks.join(', ')}`);
            missingChunks.forEach(sequenceNumber => {
                this.requestChunkRetry(sequenceNumber);
            });
        }
    }

    requestChunkRetry(sequenceNumber) {
        const retries = this.retryCount.get(sequenceNumber) || 0;

        if (retries >= this.MAX_RETRIES) {
            console.error(`Max retries exceeded for chunk ${sequenceNumber}`);
            this.handleTransferFailure();
            return;
        }

        this.retryCount.set(sequenceNumber, retries + 1);
        console.log(`Requesting retry for chunk ${sequenceNumber}, attempt ${retries + 1}`);
    }

    handleTransferFailure() {
        console.error(`Transfer ${this.transferId} failed after max retries`);
        this.onComplete(null, new Error('Transfer failed after max retries'));
        this.cleanup();
    }

    async processCompleteData() {
        try {
            const orderedData = Array.from(
                { length: this.totalChunks },
                (_, i) => this.chunks.get(i + 1)
            );

            const completeData = Buffer.concat(orderedData);
            this.onComplete(completeData, null);
            this.cleanup();
        } catch (error) {
            console.error('Error processing complete data:', error);
            this.onComplete(null, error);
            this.cleanup();
        }
    }

    cleanup() {
        this.clearChunkTimeout();
        this.chunks.clear();
        this.receivedSequences.clear();
        this.retryCount.clear();
    }
}

class IoTClient {
    constructor() {
        this.reconstructors = new Map();
        this.connection = null;
        this.pendingTransfers = new Set();
        this.client = new mqtt.MqttClient();
    }

    async connect() {
        try {
            const credentialsProvider = fromEnv();
            const credentials = await credentialsProvider();

            const builder = iot.AwsIotMqttConnectionConfigBuilder
                .new_builder_for_websocket()
                .with_clean_session(true)
                .with_client_id(`test-client-${uuidv4()}`)
                .with_endpoint(IOT_ENDPOINT)
                .with_credentials(
                    AWS_REGION,
                    credentials.accessKeyId,
                    credentials.secretAccessKey,
                    credentials.sessionToken
                )
                .with_keep_alive_seconds(30);

            const config = builder.build();
            this.connection = this.client.new_connection(config);

            return new Promise((resolve, reject) => {
                this.connection.on('connect', () => {
                    console.log('Connected to AWS IoT');
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
        } catch (error) {
            console.error('Failed to connect:', error);
            throw error;
        }
    }

    async subscribe(transferId, onComplete) {
        if (!this.connection) {
            throw new Error('Client not connected');
        }

        if (this.pendingTransfers.has(transferId)) {
            throw new Error('Transfer ID already in use');
        }

        this.pendingTransfers.add(transferId);

        const reconstructor = new ChunkReconstructor(transferId, (data, error) => {
            this.pendingTransfers.delete(transferId);
            onComplete(data, error);
        });

        this.reconstructors.set(transferId, reconstructor);

        try {
            await this.connection.subscribe(
                IOT_TOPIC,
                mqtt.QoS.AtLeastOnce
            );
            console.log(`Subscribed to topic: ${IOT_TOPIC} for transfer ${transferId}`);
        } catch (error) {
            console.error('Subscribe error:', error);
            this.pendingTransfers.delete(transferId);
            this.reconstructors.delete(transferId);
            throw error;
        }
    }

    handleMessage(topic, payloadBuffer) {
        try {
            const message = JSON.parse(Buffer.from(payloadBuffer).toString());

            if (message.type !== 'arrow_data') {
                console.log('Received non-arrow data message, ignoring');
                return;
            }

            const { metadata } = message;
            const transferId = metadata.transfer_id;

            const reconstructor = this.reconstructors.get(transferId);
            if (!reconstructor) {
                console.log(`No reconstructor found for transfer ${transferId}`);
                return;
            }

            const isComplete = reconstructor.addChunk(message);
            if (isComplete) {
                this.reconstructors.delete(transferId);
                this.pendingTransfers.delete(transferId);
            }
        } catch (error) {
            console.error('Error processing message:', error);
        }
    }

    async disconnect() {
        if (this.connection) {
            try {
                await this.connection.disconnect();
                this.connection = null;
                console.log('Disconnected from AWS IoT');
            } catch (error) {
                console.error('Error disconnecting:', error);
                throw error;
            }
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

        const { transferId, metadata } = response.data.data.getData;
        console.log(`Query ${queryId} Success: Transfer ID: ${transferId}, Expecting ${metadata.chunkCount} chunks`);

        await iotClient.subscribe(transferId, async (completeData, error) => {
            if (error) {
                console.error(`Transfer ${transferId} failed:`, error);
                return;
            }
            console.log(`Received complete data for transfer ${transferId}, size: ${completeData.length} bytes`);

            try {
                // Create a RecordBatchStreamReader from the Buffer
                const reader = await RecordBatchStreamReader.from(completeData);

                // Read all record batches
                const batches = await reader.readAll();

                // Process each batch and collect all records
                const allRecords = [];

                for (const batch of batches) {
                    // Convert each batch to an array of rows
                    const rows = batch.toArray();
                    allRecords.push(...rows);
                }

                // Replacer function to handle BigInt serialization
                function bigIntReplacer(key, value) {
                    if (typeof value === 'bigint') {
                        return value.toString();
                    } else {
                        return value;
                    }
                }

                console.log(`Data for Transfer ID ${transferId}:`);
                console.log(JSON.stringify(allRecords, bigIntReplacer, 2));

            } catch (parseError) {
                console.error(`Error parsing Arrow IPC data for transfer ${transferId}:`, parseError);
                // Additional error details for debugging
                console.error('Parse error details:', {
                    message: parseError.message,
                    stack: parseError.stack,
                    dataSize: completeData.length
                });
            }
        });

    } catch (error) {
        console.error(`Query ${queryId} Failed:`, error.message);
        throw error;
    }
}


async function startConcurrentQueries(numberOfConnections, sqlQuery, durationMs) {
    console.log(`Starting ${numberOfConnections} concurrent GraphQL queries for ${durationMs} ms...`);

    const iotClient = new IoTClient();

    try {
        await iotClient.connect();
        console.log('Connected to IoT endpoint');

        const startTime = Date.now();
        const endTime = startTime + durationMs;
        const connectionIds = Array.from({ length: numberOfConnections }, () => uuidv4());

        const queryLoop = async (connectionId) => {
            while (Date.now() < endTime) {
                await performGraphQLQuery(iotClient, sqlQuery, connectionId);
                await new Promise(resolve => setTimeout(resolve, 1000));
            }
            console.log(`Connection ${connectionId} finished`);
        };

        await Promise.all(connectionIds.map(id => queryLoop(id)));
    } catch (error) {
        console.error('Error in concurrent queries:', error);
        throw error;
    } finally {
        await iotClient.disconnect();
    }
}

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
        console.log('--- Testing Completed Successfully ---');
    } catch (error) {
        console.error('An unexpected error occurred:', error);
        process.exit(1);
    }
})();
