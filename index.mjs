import { MongoClient } from 'mongodb';
import { PubSub } from '@google-cloud/pubsub';
import avro from 'avro-js';
import fs from 'fs';

// Change these before running.
const MONGODB_URI = 'mongodb+srv://saas_staging_admin:7G2Vw3OwylVtvDTu@imp-staging-cluster.9usly.mongodb.net/saas_staging';
const PUB_SUB_TOPIC = 'projects/imp-dev-368912/topics/assets-interpolate-cdc';

let mongodbClient;
try {
    mongodbClient = new MongoClient(MONGODB_URI);
    await monitorCollectionForInserts(mongodbClient, 'saas_staging', 'assets_interpolation_data');
} finally {
    mongodbClient.close();
}

async function monitorCollectionForInserts(client, databaseName, collectionName, timeInMs) {
    const collection = client.db(databaseName).collection(collectionName);

    // An aggregation pipeline that matches on new documents in the collection.
    const pipeline = [ { $match: { operationType: 'insert' } } ];
    const changeStream = collection.watch(pipeline);
    console.log(`Watching for changes in '${databaseName}.${collectionName}'...`);

    changeStream.on('change', event => {
        console.log("Change event called.");
        const document = event.fullDocument;
        publishDocumentAsMessage(document, PUB_SUB_TOPIC);
    });

    await closeChangeStream(timeInMs, changeStream);
}

function closeChangeStream(timeInMs = 60000, changeStream) {
    return new Promise((resolve) => {
        setTimeout(() => {
            console.log('Closing the change stream');
            changeStream.close();
            resolve();
        }, timeInMs)
    })
};

async function publishDocumentAsMessage(document, topicName) {
    const pubSubClient = new PubSub();
    const topic = pubSubClient.topic(topicName);

    const definition = fs.readFileSync('./document-message.avsc').toString();
    const type = avro.parse(definition);

    const message = {
        "asset_id": document?.asset_id?.toString()
    };

    const dataBuffer = Buffer.from(type.toString(message));
    try {
        const messageId = await topic.publishMessage({ data: dataBuffer });
        console.log(`Avro record ${messageId} published.`);
    } catch(error) {
        console.error(error);
    }
}



jdbc:mongobi://testorg-test-cluster-de-biconnector-pri.5iei7.mongodb.net:27015/testorg-test-cluster-dev?zeroDateTimeBehavior=convertToNull&tcpKeepAlive=true&useSSL=true&requireSSL=true&enabledTLSProtocols=TLSv1.0,TLSv1.1,TLSv1.2,TLSv1.3&trustServerCertificate=true&allowLoadLocalInfile=false



mongodb+srv://saas_staging_admin:7G2Vw3OwylVtvDTu@imp-staging-cluster.9usly.mongodb.net/