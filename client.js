const {
    CognitoIdentityClient,
    GetIdCommand,
    GetCredentialsForIdentityCommand,
} = require("@aws-sdk/client-cognito-identity");
const { IoTClient, AttachPolicyCommand } = require("@aws-sdk/client-iot");
const { fromCognitoIdentityPool } = require("@aws-sdk/credential-provider-cognito-identity");
const { mqtt, iot } = require("aws-crt");
const { v4: uuidv4 } = require("uuid");
const dotenv = require("dotenv");
const axios = require("axios");
const gql = require("graphql-tag");
const { print } = require("graphql");
const { RecordBatchStreamReader } = require("apache-arrow");
const fs = require("fs");

// Load environment variables
dotenv.config();

const {
    COGNITO_IDENTITY_POOL_ID,
    APP_SYNC_API_URL,
    APP_SYNC_API_KEY,
    SQL_QUERY = "SELECT * FROM job_data LIMIT 1000",
    IOT_ENDPOINT,
    IOT_TOPIC,
    AWS_REGION = "us-east-1",
    SAVE_DATA = "false",
} = process.env;

// Validate essential environment variables
if (!APP_SYNC_API_URL || !APP_SYNC_API_KEY || !IOT_ENDPOINT || !IOT_TOPIC || !COGNITO_IDENTITY_POOL_ID) {
    console.error("Error: Missing required environment variables");
    process.exit(1);
}

const GET_DATA_QUERY = gql`
    query GetData($query: String!, $rowLimit: Int, $batchSize: Int, $transferId: String!) {
        getData(query: $query, rowLimit: $rowLimit, batchSize: $batchSize, transferId: $transferId) {
            transferId
            metadata {
                rowCount
                chunkCount
                schema
            }
        }
    }
`;

class StreamingReconstructor {
    constructor(transferId, onDataBatch, onComplete, timeoutMs = 300000) {
        this.transferId = transferId;
        this.onDataBatch = onDataBatch;
        this.onComplete = onComplete;
        this.isFinished = false;
        this.startTime = Date.now();
        this.schema = null;
        this.totalRows = 0;
        this.processedSequences = new Set();
        this.lastActivityTime = Date.now();
        this.chunkBuffers = {};

        // Set up inactivity timeout
        this.activityCheck = setInterval(() => {
            const inactiveTime = Date.now() - this.lastActivityTime;
            if (inactiveTime > 10000) { // 10 seconds of inactivity
                console.log(`No activity for ${inactiveTime}ms. Processing available data.`);
                this.processAvailableData();
            }
        }, 5000);

        // Set up final timeout
        this.timeout = setTimeout(() => {
            if (!this.isFinished) {
                clearInterval(this.activityCheck);
                this.processAvailableData();
                const error = new Error('Transfer timed out');
                error.context = {
                    transferId: this.transferId,
                    processedSequences: Array.from(this.processedSequences),
                    totalRowsProcessed: this.totalRows,
                    timeElapsed: Date.now() - this.startTime
                };
                this.onComplete(error);
                this.isFinished = true;
            }
        }, timeoutMs);
    }

    processAvailableData() {
        try {
            // Process any complete sequences we haven't processed yet
            for (const [sequenceNumber, bufferInfo] of Object.entries(this.chunkBuffers)) {
                if (bufferInfo.receivedChunks === bufferInfo.totalChunks &&
                    !this.processedSequences.has(parseInt(sequenceNumber))) {
                    this.processCompleteSequence(parseInt(sequenceNumber), {
                        timestamp: Date.now(),
                        is_partial: true
                    });
                }
            }
        } catch (error) {
            console.error('Error processing available data:', error);
        }
    }

    async processMessage(message) {
        if (this.isFinished) return false;

        try {
            const { metadata, data, type } = message;
            console.log(`Processing message for sequence ${metadata.sequence_number}, chunk ${metadata.chunk_number}/${metadata.total_chunks} (is_final_sequence: ${metadata.is_final_sequence})`);

            if (type !== 'arrow_data') {
                console.warn(`Unexpected message type: ${type}`);
                return false;
            }

            const sequenceNumber = metadata.sequence_number;
            const chunkNumber = metadata.chunk_number;
            const totalChunks = metadata.total_chunks;

            // Initialize storage for this sequence if needed
            if (!this.chunkBuffers[sequenceNumber]) {
                this.chunkBuffers[sequenceNumber] = {
                    chunks: new Array(totalChunks),
                    totalChunks,
                    receivedChunks: 0,
                };
            }

            const bufferInfo = this.chunkBuffers[sequenceNumber];

            // Store the chunk if we haven't stored it before
            if (!bufferInfo.chunks[chunkNumber - 1]) {
                bufferInfo.chunks[chunkNumber - 1] = data;
                bufferInfo.receivedChunks++;
                console.log(`Stored chunk ${chunkNumber}/${totalChunks} for sequence ${sequenceNumber}`);
            }

            // Process the sequence if all chunks are received
            if (bufferInfo.receivedChunks === bufferInfo.totalChunks &&
                !this.processedSequences.has(sequenceNumber)) {

                console.log(`Processing complete sequence ${sequenceNumber}`);
                await this.processCompleteSequence(sequenceNumber, metadata);

                // Clean up the processed sequence
                delete this.chunkBuffers[sequenceNumber];
                this.processedSequences.add(sequenceNumber);

                // If this was the final sequence, complete the transfer
                if (metadata.is_final_sequence === true) {
                    console.log('Final sequence detected, completing transfer');
                    await this.complete();
                    return true;
                }
            }

            return false;
        } catch (error) {
            console.error('Error processing message:', error);
            console.error('Message:', JSON.stringify(message, null, 2));
            this.onComplete(error);
            return false;
        }
    }

    async processCompleteSequence(sequenceNumber, metadata) {
        const bufferInfo = this.chunkBuffers[sequenceNumber];
        const completeData = bufferInfo.chunks.join('');
        const buffer = Buffer.from(completeData, 'base64');

        try {
            const reader = await RecordBatchStreamReader.from(buffer);
            const batches = await reader.readAll();

            if (!this.schema && batches.length > 0) {
                this.schema = batches[0].schema;
            }

            for (const batch of batches) {
                const rows = batch.toArray();
                this.totalRows += rows.length;

                this.onDataBatch({
                    rows,
                    metadata: {
                        sequence: sequenceNumber,
                        timestamp: metadata.timestamp,
                        processingTime: Date.now() - this.startTime
                    }
                });
            }

            console.log(`Processed ${batches.length} batches from sequence ${sequenceNumber}`);
        } catch (error) {
            console.error('Error processing Arrow data:', error);
            console.error('Buffer info:', bufferInfo);
            throw error;
        }
    }

    async complete() {
        if (this.isFinished) return;

        this.isFinished = true;
        clearTimeout(this.timeout);

        const processingTime = Date.now() - this.startTime;
        console.log(`Transfer completed. Processed ${this.totalRows} rows in ${processingTime}ms`);

        this.onComplete(null, {
            transferId: this.transferId,
            totalRows: this.totalRows,
            processingTime,
            processedSequences: Array.from(this.processedSequences),
            schema: this.schema?.fields.map(f => ({
                name: f.name,
                type: f.type.toString()
            }))
        });
    }
}

class IoTClientWrapper {
    constructor() {
        this.streamingReconstructors = new Map();
        this.connection = null;
        this.client = new mqtt.MqttClient();
    }

    async connect() {
        try {
            const cognitoClient = new CognitoIdentityClient({ region: AWS_REGION });
            const getIdCommand = new GetIdCommand({
                IdentityPoolId: COGNITO_IDENTITY_POOL_ID,
            });
            const { IdentityId } = await cognitoClient.send(getIdCommand);
            console.log("Cognito Identity ID:", IdentityId);

            const getCredentialsCommand = new GetCredentialsForIdentityCommand({
                IdentityId,
            });
            const credentialsResponse = await cognitoClient.send(getCredentialsCommand);
            const rawCredentials = credentialsResponse.Credentials;

            const credentials = {
                accessKeyId: rawCredentials.AccessKeyId,
                secretAccessKey: rawCredentials.SecretKey,
                sessionToken: rawCredentials.SessionToken,
                expiration: rawCredentials.Expiration ? new Date(rawCredentials.Expiration * 1000) : undefined,
            };

            const iotClient = new IoTClient({ region: AWS_REGION, credentials });
            const attachPolicyCommand = new AttachPolicyCommand({
                policyName: "DataStreamingIoTPolicy",
                target: IdentityId,
            });
            await iotClient.send(attachPolicyCommand);
            console.log("AWS IoT policy attached to identity");

            const builder = iot.AwsIotMqttConnectionConfigBuilder.new_with_websockets()
                .with_clean_session(true)
                .with_client_id(IdentityId)
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
                this.connection.on("connect", () => {
                    console.log("Connected to AWS IoT");
                    resolve();
                });

                this.connection.on("error", (error) => {
                    console.error("Connection error:", error);
                    reject(error);
                });

                this.connection.on("disconnect", () => {
                    console.log("Disconnected from AWS IoT");
                });

                this.connection.on("message", (topic, payload) => {
                    this.handleMessage(topic, payload);
                });

                this.connection.connect();
            });
        } catch (error) {
            console.error("Failed to connect:", error);
            throw error;
        }
    }

    async subscribe(transferId, onDataBatch, onComplete) {
        if (!this.connection) {
            throw new Error('Client not connected');
        }

        console.log(`Setting up subscription for transfer ${transferId}`);

        const reconstructor = new StreamingReconstructor(
            transferId,
            onDataBatch,
            onComplete
        );

        this.streamingReconstructors.set(transferId, reconstructor);

        try {
            await this.connection.subscribe(
                IOT_TOPIC,
                mqtt.QoS.AtLeastOnce
            );
            console.log(`Subscribed to topic: ${IOT_TOPIC}`);
        } catch (error) {
            console.error('Subscribe error:', error);
            this.streamingReconstructors.delete(transferId);
            throw error;
        }
    }

    async handleMessage(topic, payloadBuffer) {
        try {
            // Convert ArrayBuffer to string properly
            let payload;
            if (payloadBuffer instanceof ArrayBuffer) {
                payload = new TextDecoder().decode(payloadBuffer);
            } else if (Buffer.isBuffer(payloadBuffer)) {
                payload = payloadBuffer.toString();
            } else {
                payload = payloadBuffer.toString();
            }

            const message = JSON.parse(payload);

            if (!message || !message.metadata) {
                console.warn('Received invalid message format:', message);
                return;
            }

            console.log(`Received message for sequence ${message.metadata.sequence_number}`);

            // Process message with all active reconstructors
            for (const reconstructor of this.streamingReconstructors.values()) {
                await reconstructor.processMessage(message);
            }
        } catch (error) {
            console.error('Error handling message:', error);
            console.error('Raw payload:', payloadBuffer);
        }
    }

    async testConnection() {
        if (!this.connection) {
            throw new Error('Not connected');
        }

        try {
            await this.connection.publish(
                IOT_TOPIC,
                JSON.stringify({ test: 'connectivity' }),
                mqtt.QoS.AtLeastOnce
            );
            console.log('Test message published successfully');
        } catch (error) {
            console.error('Failed to publish test message:', error);
            throw error;
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

async function performStreamingQuery(iotClient, query, queryId) {
    try {
        const transferId = uuidv4();
        console.log(`Starting query execution with transfer ID: ${transferId}`);

        // Set up data reception promise
        const dataReceptionPromise = new Promise((resolve, reject) => {
            const batchHandler = (batchData) => {
                console.log(`Received batch with ${batchData.rows.length} rows for sequence ${batchData.metadata.sequence}`);
            };

            const completionHandler = (error, stats) => {
                if (error) {
                    console.error(`Transfer failed:`, error);
                    reject(error);
                } else {
                    console.log(`Transfer completed:`, stats);
                    resolve(stats);
                }
            };

            iotClient.subscribe(transferId, batchHandler, completionHandler)
                .catch(reject);
        });

        // Wait for subscription to be ready
        await new Promise(resolve => setTimeout(resolve, 1000));

        // Execute GraphQL query with required transferId
        const variables = {
            query,
            rowLimit: 100000,
            batchSize: 100,
            transferId // Required field
        };

        console.log('Executing GraphQL query with variables:', variables);

        const response = await axios.post(
            APP_SYNC_API_URL,
            {
                query: print(GET_DATA_QUERY),
                variables
            },
            {
                headers: {
                    'Content-Type': 'application/json',
                    'x-api-key': APP_SYNC_API_KEY,
                },
            }
        );

        if (response.data.errors) {
            throw new Error(`GraphQL query failed: ${JSON.stringify(response.data.errors)}`);
        }

        console.log('Query initiated successfully, waiting for data...');

        // Wait for data reception to complete
        const result = await dataReceptionPromise;
        return result;

    } catch (error) {
        console.error(`Query ${queryId} failed:`, error);
        throw error;
    }
}

async function startSingleQuery(sqlQuery) {
    console.log(`\n--- Starting streaming query ---`);
    const iotClient = new IoTClientWrapper();

    try {
        await iotClient.connect();
        console.log('Connected to IoT endpoint');

        await iotClient.testConnection();
        console.log('Connection test successful');

        const queryId = 'single-query';
        const result = await performStreamingQuery(iotClient, sqlQuery, queryId);
        console.log(`Query completed successfully:`, result);

    } catch (error) {
        console.error('Error in query execution:', error);
        throw error;
    } finally {
        try {
            await iotClient.disconnect();
            console.log('Disconnected from IoT endpoint');
        } catch (disconnectError) {
            console.error('Error during disconnect:', disconnectError);
        }
    }
}

(async () => {
    console.log('--- Streaming Lambda Tester ---');
    console.log(`AppSync API URL: ${APP_SYNC_API_URL}`);
    console.log(`SQL Query: ${SQL_QUERY}`);
    console.log(`Save Data: ${SAVE_DATA === 'true' ? 'Enabled' : 'Disabled'}\n`);

    try {
        await startSingleQuery(SQL_QUERY);
        console.log('\n--- Testing Completed Successfully ---');
        process.exit(0);
    } catch (error) {
        console.error('Testing failed:', error);
        process.exit(1);
    }
})();