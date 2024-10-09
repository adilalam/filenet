const AWS = require('aws-sdk');
const axios = require('axios');
const stream = require('stream');
const util = require('util');

// Promisify the pipeline to handle stream errors more cleanly
const pipeline = util.promisify(stream.pipeline);

// Configure AWS SDK for S3
const s3 = new AWS.S3({
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    region: process.env.AWS_REGION
});

// Function to retrieve the file list from FileNet (metadata or URLs)
async function getFileListFromFileNet() {
    // Replace this with your actual FileNet logic for fetching file URLs
    // This should return an array of file URLs or metadata
    const fileList = [
        'https://your-filenet-endpoint.com/file/1',
        'https://your-filenet-endpoint.com/file/2',
        // Add more file URLs...
    ];
    return fileList;
}

// Function to stream file from FileNet
async function getFileFromFileNet(fileNetUrl, retryCount = 3) {
    let attempt = 0;
    while (attempt < retryCount) {
        try {
            const response = await axios({
                method: 'get',
                url: fileNetUrl,
                responseType: 'stream',
                headers: {
                    'Authorization': `Bearer ${process.env.FILENET_ACCESS_TOKEN}`, // Bearer token for authentication
                },
                timeout: 10000,  // 10 second timeout
            });

            if (response.status === 200) {
                return response.data;  // This is the file stream
            } else {
                throw new Error(`Failed to download file. Status: ${response.status}`);
            }
        } catch (error) {
            attempt++;
            console.error(`Error downloading file from FileNet (${fileNetUrl}): ${error.message}. Retrying...`);
            if (attempt >= retryCount) {
                throw new Error(`Exceeded retry attempts for downloading file from FileNet: ${fileNetUrl}`);
            }
        }
    }
}

// Function to upload file stream to S3
async function uploadFileToS3(fileStream, bucketName, key, retryCount = 3) {
    let attempt = 0;
    while (attempt < retryCount) {
        try {
            console.log(`Uploading file ${key} to S3 (attempt ${attempt + 1})`);

            const passThroughStream = new stream.PassThrough();

            const params = {
                Bucket: bucketName,
                Key: key,
                Body: passThroughStream,
            };

            const uploadPromise = s3.upload(params).promise();
            await pipeline(fileStream, passThroughStream);

            const result = await uploadPromise;
            console.log(`File uploaded successfully to S3: ${key}`);
            return result;
        } catch (error) {
            attempt++;
            console.error(`Error uploading file to S3 (${key}): ${error.message}. Retrying...`);
            if (attempt >= retryCount) {
                throw new Error(`Exceeded retry attempts for uploading file to S3: ${key}`);
            }
        }
    }
}

// Function to process a single file: download from FileNet and upload to S3
async function processSingleFile(fileNetUrl, bucketName, s3Key) {
    try {
        // 1. Download file from FileNet
        const fileStream = await getFileFromFileNet(fileNetUrl);

        // 2. Upload to S3
        await uploadFileToS3(fileStream, bucketName, s3Key);
    } catch (error) {
        console.error(`Failed to process file ${fileNetUrl}:`, error.message);
    }
}

// Main function to process files in chunks with limited concurrency
async function processFilesInChunks(bucketName, chunkSize = 10, concurrencyLimit = 5) {
    try {
        // 1. Get the list of files from FileNet
        const fileList = await getFileListFromFileNet();
        const totalFiles = fileList.length;
        console.log(`Total files to process: ${totalFiles}`);

        // 2. Process files in chunks
        for (let i = 0; i < totalFiles; i += chunkSize) {
            const fileChunk = fileList.slice(i, i + chunkSize);
            console.log(`Processing files ${i + 1} to ${i + fileChunk.length}`);

            // 3. Process each chunk with concurrency control
            await processChunkWithConcurrency(fileChunk, bucketName, concurrencyLimit);
        }

        console.log('All files have been processed.');
    } catch (error) {
        console.error('Error during file processing:', error.message);
    }
}

// Function to process a chunk of files with limited concurrency
async function processChunkWithConcurrency(fileChunk, bucketName, concurrencyLimit) {
    // Create an array of promises with limited concurrency
    const promises = [];
    for (const fileUrl of fileChunk) {
        const s3Key = `your-s3-folder/${fileUrl.split('/').pop()}`;  // Generate an S3 key from the file URL

        // If we reach concurrency limit, wait for ongoing uploads to finish
        if (promises.length >= concurrencyLimit) {
            await Promise.all(promises);
            promises.length = 0;  // Reset the array
        }

        // Process each file asynchronously
        const promise = processSingleFile(fileUrl, bucketName, s3Key);
        promises.push(promise);
    }

    // Wait for all remaining files in the chunk to complete
    await Promise.all(promises);
}

// Example usage
const bucketName = 'your-s3-bucket';  // S3 bucket where files will be uploaded
const chunkSize = 10;                 // Number of files to process in one chunk
const concurrencyLimit = 5;           // Number of files to process concurrently

processFilesInChunks(bucketName, chunkSize, concurrencyLimit);
