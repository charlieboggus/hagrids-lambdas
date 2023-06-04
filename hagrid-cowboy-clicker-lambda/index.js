const AWS = require('aws-sdk');
const dynamo = new AWS.DynamoDB.DocumentClient();
const cowboyToken = process.env.COWBOY_TOKEN;
const tableName = process.env.TABLE_NAME;

exports.handler = async (event, context) => {
    const token = event.body.token;
    if (token !== cowboyToken) {
        return { statusCode: '200' };
    }
    const updateCommand = {
        TableName: tableName,
        Key: {
            clickToken: cowboyToken,
        },
        UpdateExpression: "SET clickCount = if_not_exists(clickCount, :initial) + :amount",
        ExpressionAttributeValues: {
            ":initial": 0,
            ":amount": 1,
        },
        ReturnValues: 'UPDATED_NEW',
    };
    const response = await dynamo.update(updateCommand, (err, data) => {
        if (err) {
            console.log(`Error: ${err}`);
        }
        else {
            console.log(`Data: ${data}`);
        }
    }).promise();
    return { statusCode: '200' };
};
