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
    SQL_QUERY = "SELECT * FROM job_data LIMIT 100",
    IOT_ENDPOINT,
    IOT_TOPIC,
    AWS_REGION = "us-east-1",
    SAVE_DATA = "false", // Ensure default value if not set
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
    constructor(transferId, onDataBatch, onComplete, timeoutMs = 120000) {
        this.transferId = transferId;
        this.onDataBatch = onDataBatch;
        this.onComplete = onComplete;
        this.isFinished = false;
        this.startTime = Date.now();
        this.schema = null;
        this.totalRows = 0;

        // Initialize storage for chunks
        this.chunkBuffers = {}; // { sequence_number: { chunks: [], total_chunks, received_chunks } }

        // Set up timeout to handle cases where no data is received
        this.timeout = setTimeout(() => {
            if (!this.isFinished) {
                this.onComplete(new Error('Timeout: No data received within the expected time.'));
                this.isFinished = true;
            }
        }, timeoutMs);
    }

    async processMessage(message) {
        if (this.isFinished) return false;

        try {
            const { metadata, data } = message;

            if (metadata.transfer_id !== this.transferId) {
                console.warn(`Received message for unknown transferId: ${metadata.transfer_id}`);
                return false;
            }

            const sequenceNumber = metadata.sequence_number;
            const chunkNumber = metadata.chunk_number;
            const totalChunks = metadata.total_chunks;

            // Initialize storage for this sequence number if not already done
            if (!this.chunkBuffers[sequenceNumber]) {
                this.chunkBuffers[sequenceNumber] = {
                    chunks: [],
                    totalChunks,
                    receivedChunks: 0,
                };
            }

            const bufferInfo = this.chunkBuffers[sequenceNumber];
            bufferInfo.chunks[chunkNumber - 1] = data; // Store chunk in the correct order (zero-based index)
            bufferInfo.receivedChunks++;

            console.log(`Received chunk ${chunkNumber}/${totalChunks} for sequence ${sequenceNumber}`);

            // Check if all chunks have been received
            if (bufferInfo.receivedChunks === totalChunks) {
                // Reassemble the complete data
                const completeData = bufferInfo.chunks.join(''); // Concatenate base64 strings

                // Deserialize the Arrow Stream IPC data
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

                    // Clean up the buffer for this sequence number
                    delete this.chunkBuffers[sequenceNumber];

                    // Check if this was the final sequence
                    if (metadata.is_final_sequence) {
                        await this.complete();
                        return true;
                    }

                } catch (error) {
                    console.error('Error processing Arrow data:', error);
                    throw error;
                }
            }

            return false;
        } catch (error) {
            console.error('Error in processMessage:', error);
            this.onComplete(new Error(`Failed to process message: ${error.message}`));
            return false;
        }
    }

    async complete() {
        if (this.isFinished) return;

        this.isFinished = true;
        clearTimeout(this.timeout); // Clear timeout upon completion

        const processingTime = Date.now() - this.startTime;

        this.onComplete(null, {
            transferId: this.transferId,
            totalRows: this.totalRows,
            processingTime,
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
        this.currentTransferId = null; // Track current transferId
    }

    async connect() {
        try {
            // Initialize Cognito Identity Client
            const cognitoClient = new CognitoIdentityClient({ region: AWS_REGION });

            // Get Cognito Identity ID
            const getIdCommand = new GetIdCommand({
                IdentityPoolId: COGNITO_IDENTITY_POOL_ID,
            });
            const { IdentityId } = await cognitoClient.send(getIdCommand);
            console.log("Cognito Identity ID:", IdentityId);

            // Get AWS Credentials
            const getCredentialsCommand = new GetCredentialsForIdentityCommand({
                IdentityId,
            });
            const credentialsResponse = await cognitoClient.send(getCredentialsCommand);
            const rawCredentials = credentialsResponse.Credentials;

            // Map credentials to the expected format
            const credentials = {
                accessKeyId: rawCredentials.AccessKeyId,
                secretAccessKey: rawCredentials.SecretKey,
                sessionToken: rawCredentials.SessionToken,
                expiration: rawCredentials.Expiration ? new Date(rawCredentials.Expiration * 1000) : undefined,
            };

            // Attach AWS IoT policy to the identity
            const iotClient = new IoTClient({ region: AWS_REGION, credentials });

            const attachPolicyCommand = new AttachPolicyCommand({
                policyName: "DataStreamingIoTPolicy", // Must match the name in aws_iot_policy
                target: IdentityId,
            });
            await iotClient.send(attachPolicyCommand);
            console.log("AWS IoT policy attached to identity");

            // Use the Cognito Identity ID as the MQTT client ID
            const clientId = IdentityId;

            const builder = iot.AwsIotMqttConnectionConfigBuilder.new_with_websockets()
                .with_clean_session(true)
                .with_client_id(clientId)
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

        this.currentTransferId = transferId; // Set current transferId

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
            console.log(`Subscribed to topic: ${IOT_TOPIC} for transfer ${transferId}`);
        } catch (error) {
            console.error('Subscribe error:', error);
            this.streamingReconstructors.delete(transferId);
            throw error;
        }
    }

    async handleMessage(topic, payloadBuffer) {
        try {
            const message = JSON.parse(Buffer.from(payloadBuffer).toString());
            console.log(`Received message on topic ${topic}:`, message.metadata);

            if (!message || !message.metadata || !message.metadata.transfer_id) {
                console.warn('Received invalid message format');
                return;
            }

            const reconstructor = this.streamingReconstructors.get(message.metadata.transfer_id);
            if (!reconstructor) {
                console.warn(`No reconstructor found for transfer ID: ${message.metadata.transfer_id}`);
                return;
            }

            await reconstructor.processMessage(message);
        } catch (error) {
            console.error('Error processing message:', error);
        }
    }

    // Add debugging method
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
        // Generate transfer ID early
        const transferId = uuidv4();

        // Subscribe BEFORE making the GraphQL query
        console.log(`Setting up subscription for transfer ${transferId}...`);
        await iotClient.subscribe(
            transferId,
            (batchData) => {
                console.log(`Received batch data for sequence ${batchData.metadata.sequence}`);
                console.log('Batch data:', batchData.rows);

                // Save data if enabled
                if (process.env.SAVE_DATA === 'true') {
                    const filename = `data_${transferId}_batch_${batchData.metadata.sequence}.json`;
                    fs.writeFileSync(filename, JSON.stringify(batchData.rows, null, 2));
                    console.log(`Data saved to ${filename}`);
                }
            },
            (error, stats) => {
                if (error) {
                    console.error(`Query ${queryId} failed:`, error);
                    return;
                }
                console.log(`Query ${queryId} completed:`, stats);
            }
        );

        // Wait a moment to ensure subscription is ready
        await new Promise(resolve => setTimeout(resolve, 1000));

        const variables = {
            query,
            rowLimit: 100,
            batchSize: 100,
            transferId // Include transferId in variables
        };

        console.log('Sending GraphQL query with variables:', variables);

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
            console.error(`Query ${queryId} GraphQL Errors:`, response.data.errors);
            throw new Error('GraphQL query failed.');
        }

        console.log(`GraphQL query response:`, response.data);

        // Return a Promise that resolves when the data transfer is complete
        return new Promise((resolve, reject) => {
            // We've already set up the subscription, just need to wait for completion
            const timeout = setTimeout(() => {
                reject(new Error('Query timed out'));
            }, 120000); // 2 minute timeout

            const reconstructor = iotClient.streamingReconstructors.get(transferId);
            if (!reconstructor) {
                clearTimeout(timeout);
                reject(new Error(`No reconstructor found for transfer ID: ${transferId}`));
                return;
            }

            reconstructor.onComplete = (error, stats) => {
                clearTimeout(timeout);
                if (error) {
                    reject(error);
                } else {
                    resolve(stats);
                }
            };
        });
    } catch (error) {
        console.error(`Query ${queryId} failed:`, error);
        throw error;
    }
}


async function startSingleQuery(sqlQuery) {
    console.log(`\n--- Starting a streaming query ---`);

    const iotClient = new IoTClientWrapper();

    try {
        await iotClient.connect();
        console.log('Connected to IoT endpoint');

        // Test connection
        await iotClient.testConnection();
        console.log('Connection test successful');

        const queryId = 'single-query';
        const result = await performStreamingQuery(iotClient, sqlQuery, queryId);
        console.log('Query completed successfully:', result);

    } catch (error) {
        console.error('Error in single query:', error);
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
    } catch (error) {
        console.error('Testing failed:', error);
        process.exit(1);
    }
})();