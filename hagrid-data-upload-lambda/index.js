const AWS = require('aws-sdk');
const documentClient = new AWS.DynamoDB.DocumentClient();
const s3 = new AWS.S3({ region: 'us-east-1' });

const tableName = process.env.TABLE_NAME;
const messageBucket = process.env.MESSAGE_BUCKET;
const voiceBucket = process.env.VOICE_BUCKET;
const messageBucketTest = process.env.MESSAGE_BUCKET_TEST;

const buildS3Params = (event) => {
    const bucket = event.Records[0].s3.bucket.name;
    const key = decodeURIComponent(event.Records[0].s3.object.key.replace(/\+/g, ' '));
    const s3Params = {
        Bucket: bucket,
        Key: key
    };
    return s3Params;
};

const storeMessageData = async (fileJson) => {
    const messageDataMap = new Map();
    const usersInBatch = [];
    
    // Build our message data map
    fileJson.forEach(message => {
        if (!messageDataMap.has(message.authorId)) {
            const userMessageData = {
                messageCount: 0,
                messages: [],
                name: message.author
            };
            messageDataMap.set(message.authorId, userMessageData);
            usersInBatch.push(message.authorId);
        }
        else {
            const userMessageData = messageDataMap.get(message.authorId);
            userMessageData.messageCount = userMessageData.messageCount + 1;
            userMessageData.messages.push(message.message);
            messageDataMap.set(message.authorId, userMessageData);
        }
    });
    console.log('Built message data map');
    
    // upload the data for each user to dynamo
    for (const user of usersInBatch) {
        const userMessageData = messageDataMap.get(user);
        console.log(`trying to update dynamo table for ${user} - ${JSON.stringify(userMessageData)}`);
        const params = {
            TableName: tableName,
            Key: {
                "userId": user
            },
            UpdateExpression: 'SET username = :username, messageCount = if_not_exists(messageCount, :initial) + :messageCount, messages = list_append(if_not_exists(messages, :emptyList), :messages)',
            ExpressionAttributeValues: {
                ':username': userMessageData.name,
                ':messageCount': userMessageData.messageCount,
                ':messages': userMessageData.messages,
                ':initial': 0,
                ':emptyList': [],
            },
            ReturnValues: "UPDATED_NEW"
        };
        await documentClient.update(params, (err, data) => {
            if (err) {
                console.log(`Error: ${err}`);
            }
            else {
                console.log(`Stored data in table: ${data}`);
            }
        }).promise();
    }
};

const storeVoiceData = async (fileJson) => {
    let statusCode = '200';
    return statusCode;
};

exports.handler = async (event, context) => {
    //console.log('Received event:', JSON.stringify(event, null, 2));
    let statusCode = '200';
    const headers = {
        'Content-Type': 'application/json',
    };
    const s3Params = buildS3Params(event);
    console.log(JSON.stringify(s3Params));
    const s3FileData = await s3.getObject(s3Params).promise();
    const s3FileJson = JSON.parse(s3FileData.Body);
    console.log(`Received JSON: ${JSON.stringify(s3FileJson)}`);
    
    if (s3Params.Bucket === messageBucket) {
        await storeMessageData(s3FileJson);
    }
    else if (s3Params.Bucket === voiceBucket) {
        //statusCode = await storeVoiceData(s3FileJson);
    }
    console.log('done');
    return { statusCode, headers };
};
