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

        // Use Map instead of object for better performance with large datasets
        this.chunkBuffers = new Map();

        // Preallocate buffer for better memory efficiency
        this.decodingBuffer = Buffer.alloc(1024 * 1024); // 1MB initial buffer

        // Batch processing optimization
        this.batchQueue = [];
        this.batchSize = 5000; // Process in larger batches
        this.processingLock = false;

        // More aggressive cleanup
        this.cleanupInterval = setInterval(() => {
            const now = Date.now();
            for (const [seq, buffer] of this.chunkBuffers) {
                if (now - buffer.timestamp > 10000) { // 10s timeout
                    this.chunkBuffers.delete(seq);
                }
            }
        }, 5000);

        // Faster activity checking
        this.activityCheck = setInterval(() => {
            const inactiveTime = Date.now() - this.lastActivityTime;
            if (inactiveTime > 2000) { // Reduced to 2s
                this.processAvailableData();
            }
        }, 1000);

        // Set timeout
        this.timeout = setTimeout(() => {
            if (!this.isFinished) {
                this.processAvailableData();
                setTimeout(() => this.complete(), 2000);
            }
        }, timeoutMs - 2000);
    }

    async processMessage(message) {
        if (this.isFinished) return false;

        try {
            const { metadata, data, type } = message;
            this.lastActivityTime = Date.now();

            if (type !== 'arrow_data') return false;

            const sequenceNumber = metadata.sequence_number;
            const chunkNumber = metadata.chunk_number;
            const totalChunks = metadata.total_chunks;

            // Use Map for better performance
            if (!this.chunkBuffers.has(sequenceNumber)) {
                this.chunkBuffers.set(sequenceNumber, {
                    chunks: new Array(totalChunks),
                    totalChunks,
                    receivedChunks: 0,
                    timestamp: Date.now()
                });
            }

            const bufferInfo = this.chunkBuffers.get(sequenceNumber);

            // Store chunk data
            if (!bufferInfo.chunks[chunkNumber - 1]) {
                bufferInfo.chunks[chunkNumber - 1] = data;
                bufferInfo.receivedChunks++;

                // Process immediately if sequence is complete
                if (bufferInfo.receivedChunks === bufferInfo.totalChunks) {
                    this.batchQueue.push({ sequenceNumber, metadata });

                    // Process batch if queue is full
                    if (this.batchQueue.length >= 10) {
                        await this.processBatchQueue();
                    }

                    if (metadata.is_final_sequence === true) {
                        await this.processBatchQueue();
                        await this.complete();
                        return true;
                    }
                }
            }

            return false;
        } catch (error) {
            console.error('Error processing message:', error);
            throw error;
        }
    }

    async processBatchQueue() {
        if (this.processingLock || this.batchQueue.length === 0) return;

        this.processingLock = true;
        const batchesToProcess = this.batchQueue.splice(0, 10); // Process up to 10 sequences at once

        try {
            await Promise.all(batchesToProcess.map(async ({ sequenceNumber, metadata }) => {
                await this.processCompleteSequence(sequenceNumber, metadata);
                this.chunkBuffers.delete(sequenceNumber);
                this.processedSequences.add(sequenceNumber);
            }));
        } finally {
            this.processingLock = false;
        }
    }

    async processCompleteSequence(sequenceNumber, metadata) {
        const bufferInfo = this.chunkBuffers.get(sequenceNumber);
        if (!bufferInfo) return;

        const completeData = bufferInfo.chunks.join('');
        if (!completeData) return;

        // Optimize buffer handling
        const compressedBuffer = Buffer.from(completeData, 'base64');

        // Use streaming decompression for better memory usage
        const pako = require('pako');
        const inflate = new pako.Inflate();

        try {
            inflate.push(compressedBuffer, true);
            if (inflate.err) throw new Error(inflate.msg);

            const decompressedBuffer = Buffer.from(inflate.result);

            // Process Arrow data in streaming fashion
            const reader = await RecordBatchStreamReader.from(decompressedBuffer);

            while (true) {
                const batch = await reader.next();
                if (batch.done) break;

                const rows = batch.value.toArray();
                this.totalRows += rows.length;

                this.onDataBatch({
                    rows,
                    metadata: {
                        sequence: sequenceNumber,
                        timestamp: metadata.timestamp,
                        processingTime: Date.now() - this.startTime,
                        is_partial: false
                    }
                });
            }
        } catch (error) {
            console.error(`Error processing sequence ${sequenceNumber}:`, error);
            throw error;
        }
    }

    async processAvailableData() {
        if (this.processingLock) return;
        await this.processBatchQueue();
    }

    async complete() {
        if (this.isFinished) return;

        clearTimeout(this.timeout);
        clearInterval(this.activityCheck);
        clearInterval(this.cleanupInterval);

        // Process any remaining data
        await this.processBatchQueue();

        const stats = {
            transferId: this.transferId,
            totalRows: this.totalRows,
            processingTime: Date.now() - this.startTime,
            processedSequences: Array.from(this.processedSequences),
            schema: this.schema?.fields.map(f => ({
                name: f.name,
                type: f.type.toString()
            }))
        };

        this.isFinished = true;
        this.onComplete(null, stats);
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

async function performPaginatedStreamingQuery(iotClient, query, queryId, options = {}) {
    const {
        pageSize = 50000,    // Size of each page
        maxPages = 20,       // Maximum number of pages to attempt
        maxRows = 1000000,   // Hard limit of 1M rows
        pageTimeout = 120000,
        concurrentPages = 2
    } = options;

    const results = {
        totalRows: 0,
        processedPages: 0,
        errors: [],
        stats: new Map(),  // Use Map to ensure unique page entries
        completedPages: new Set()  // Track completed pages
    };

    try {
        const baseQuery = query.replace(/\\n/g, ' ').trim();
        const semaphore = new Set();
        let currentOffset = 0;
        let hasMore = true;
        let pageNumber = 0;

        while (hasMore &&
        pageNumber < maxPages &&
        results.totalRows < maxRows) {

            while (semaphore.size >= concurrentPages) {
                await new Promise(resolve => setTimeout(resolve, 100));
            }

            const offset = currentOffset;
            const endOffset = Math.min(offset + pageSize, maxRows);  // Respect maxRows

            // Create the pagination query
            const paginatedQuery = `
                WITH numbered_rows AS (
                    SELECT ROW_NUMBER() OVER () as row_num, t.*
                    FROM (${baseQuery}) t
                )
                SELECT *
                FROM numbered_rows 
                WHERE row_num > ${offset} AND row_num <= ${endOffset}`;

            console.log(`Processing page ${pageNumber}: ${offset} to ${endOffset} (Total rows so far: ${results.totalRows})`);

            const pagePromise = (async () => {
                try {
                    const transferId = `${queryId}-page-${pageNumber}`;
                    let pageRows = 0;

                    const dataReceptionPromise = new Promise((resolve, reject) => {
                        const batchHandler = (batchData) => {
                            if (!results.completedPages.has(pageNumber)) {
                                pageRows += batchData.rows.length;

                                // Update running total, respecting maxRows
                                const newTotal = Math.min(results.totalRows + pageRows, maxRows);
                                if (newTotal >= maxRows) {
                                    hasMore = false;
                                }
                            }
                        };

                        const completionHandler = (error, stats) => {
                            if (error) {
                                reject(error);
                            } else {
                                if (!results.completedPages.has(pageNumber)) {
                                    results.completedPages.add(pageNumber);
                                    results.totalRows = Math.min(results.totalRows + pageRows, maxRows);
                                    results.stats.set(pageNumber, {
                                        page: pageNumber,
                                        offset,
                                        endOffset,
                                        rows: pageRows,
                                        status: 'COMPLETED'
                                    });
                                }
                                resolve({ rows: pageRows, status: 'COMPLETED', stats });
                            }
                        };

                        iotClient.subscribe(transferId, batchHandler, completionHandler)
                            .catch(reject);
                    });

                    const timeoutPromise = new Promise((_, reject) => {
                        setTimeout(() => {
                            reject(new Error(`Page ${pageNumber} timed out`));
                        }, pageTimeout);
                    });

                    await new Promise(resolve => setTimeout(resolve, 500));

                    const response = await axios.post(
                        APP_SYNC_API_URL,
                        {
                            query: print(GET_DATA_QUERY),
                            variables: {
                                query: paginatedQuery,
                                rowLimit: pageSize,
                                batchSize: 1000,
                                transferId
                            }
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

                    const result = await Promise.race([
                        dataReceptionPromise,
                        timeoutPromise
                    ]);

                    results.processedPages++;

                    // Stop if we got fewer rows than expected
                    if (result.rows < pageSize || results.totalRows >= maxRows) {
                        hasMore = false;
                        console.log(`End of data reached at ${results.totalRows} total rows`);
                    }

                    return result;

                } catch (error) {
                    results.errors.push({
                        page: pageNumber,
                        error: error.message
                    });
                    throw error;
                } finally {
                    semaphore.delete(pagePromise);
                }
            })();

            semaphore.add(pagePromise);
            currentOffset += pageSize;
            pageNumber++;

            await new Promise(resolve => setTimeout(resolve, 500));
        }

        await Promise.all([...semaphore]);

        console.log(`Query completed with ${results.totalRows} total rows across ${results.processedPages} pages`);
        return {
            ...results,
            stats: Array.from(results.stats.values())  // Convert Map to Array for output
        };

    } catch (error) {
        console.error(`Query ${queryId} failed:`, error);
        throw error;
    }
}

async function startSingleQuery(sqlQuery, options = {}) {
    console.log(`\n--- Starting paginated streaming query ---`);
    const iotClient = new IoTClientWrapper();

    try {
        await iotClient.connect();
        console.log('Connected to IoT endpoint');

        await iotClient.testConnection();
        console.log('Connection test successful');

        const queryId = 'single-query';

        // Set up pagination options with defaults
        const queryOptions = {
            pageSize: options.pageSize || 50000,        // Rows per page
            maxRows: options.maxRows || 1000000,        // Total maximum rows to process
            maxPages: options.maxPages || 20,           // Maximum number of pages to attempt
            pageTimeout: options.pageTimeout || 120000, // Timeout per page (2 minutes)
            concurrentPages: options.concurrentPages || 2 // Number of concurrent page requests
        };

        console.log('Query options:', {
            ...queryOptions,
            query: sqlQuery.slice(0, 100) + '...' // Show truncated query for logging
        });

        const result = await performPaginatedStreamingQuery(
            iotClient,
            sqlQuery,
            queryId,
            queryOptions
        );

        console.log('\nQuery Results:');
        console.log(`Total rows processed: ${result.totalRows}`);
        console.log(`Pages processed: ${result.processedPages}`);
        console.log(`Average rows per page: ${Math.round(result.totalRows / result.processedPages)}`);

        if (result.stats.length > 0) {
            console.log('\nPage Statistics:');
            result.stats.forEach(stat => {
                console.log(`Page ${stat.page}: ${stat.rows} rows (${stat.offset} → ${stat.endOffset}) [${stat.status}]`);
            });
        }

        if (result.errors.length > 0) {
            console.log('\nErrors encountered:', result.errors);
        }

        return result;

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

// Usage examples:
(async () => {
    console.log('--- Streaming Lambda Tester ---');
    console.log(`AppSync API URL: ${APP_SYNC_API_URL}`);
    console.log(`SQL Query: ${SQL_QUERY}`);

    try {
        // Basic usage with defaults
        // await startSingleQuery(SQL_QUERY);

        // Configuration for large dataset
        await startSingleQuery(SQL_QUERY, {
            pageSize: 100000,      // 100k rows per page
            maxRows: 1000000,      // Up to 1M total rows
            maxPages: 50,          // Up to 50 pages
            concurrentPages: 4,    // Process 4 pages at once
            pageTimeout: 180000    // 3 minutes per page
        });

        // small batch configuration
        // await startSingleQuery(SQL_QUERY, {
        //     pageSize: 10000,       // 10k rows per page
        //     maxRows: 100000,       // Up to 100k total rows
        //     maxPages: 10,          // Up to 10 pages
        //     concurrentPages: 2,    // Process 2 pages at once
        //     pageTimeout: 60000     // 1 minute per page
        // });

        console.log('\n--- Testing Completed Successfully ---');
        process.exit(0);
    } catch (error) {
        if (error.context && error.context.processingStatus === "TIMEOUT") {
            console.log("Query timed out but processed data:", error.context);
        } else {
            console.error("Query failed:", error);
        }
        process.exit(1);
    }
})();