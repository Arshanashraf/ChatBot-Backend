import { WebSocketServer, WebSocket } from "ws";
import { Kafka } from "kafkajs";
import { GoogleGenerativeAI } from "@google/generative-ai";
import { MongoClient } from "mongodb";
import amqp from "amqplib";
import dotenv from "dotenv";

dotenv.config();

// MongoDB Atlas Configuration
const mongoClient = new MongoClient(process.env.MONGO_URI);
const dbName = "chatDB";
const collectionName = "messages";
let db, messagesCollection;

// Retry Utility Function
async function retryOperation(operation, retries = 5, delay = 3000) {
    for (let attempt = 1; attempt <= retries; attempt++) {
        try {
            return await operation();
        } catch (error) {
            console.error(`Retrying ${operation.name} (Attempt ${attempt}/${retries})...`);
            await new Promise((res) => setTimeout(res, delay));
        }
    }
    throw new Error(`${operation.name} failed after ${retries} attempts`);
}

// Connect to MongoDB Atlas with Retry
async function connectToMongoDB() {
    await retryOperation(async () => {
        await mongoClient.connect();
        db = mongoClient.db(dbName);
        messagesCollection = db.collection(collectionName);
        console.log("Connected to MongoDB Atlas");
    });
}
connectToMongoDB();

// Initialize Gemini AI
const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);

async function getGeminiResponse(prompt) {
    try {
        const model = genAI.getGenerativeModel({ model: "gemini-1.5-pro" });
        const result = await model.generateContent(prompt);

        if (!result || !result.response || !result.response.text) {
            throw new Error("Invalid response from Gemini API");
        }

        return cleanGeminiResponse(result.response.text());
    } catch (error) {
        console.error("Gemini API Error:", error);
        return `Error: ${error.message || "Unknown error"}`;
    }
}

// Function to clean Gemini's response formatting
function cleanGeminiResponse(response) {
    return response.replace(/```json|```/g, "").trim();
}

// Kafka Configuration
const kafka = new Kafka({
    clientId: "websocket-server",
    brokers: ["localhost:9092"],
});

const kafkaProducer = kafka.producer();
const dbConsumer = kafka.consumer({ groupId: "dbConsumerGroupTest" });
const pubSubConsumer = kafka.consumer({ groupId: "pubSubConsumerGroup" });

// Start Kafka Producer with Retry
async function startKafkaProducer() {
    await retryOperation(async () => {
        await kafkaProducer.connect();
        console.log("Kafka Producer Connected");
    });
}
startKafkaProducer();

// RabbitMQ Configuration
let rabbitMQChannel;

async function connectRabbitMQ() {
    await retryOperation(async () => {
        const connection = await amqp.connect("amqp://localhost:5672");
        rabbitMQChannel = await connection.createChannel();
        await rabbitMQChannel.assertQueue("dbUpdates");
        await rabbitMQChannel.assertQueue("pubSubMessages");
        console.log("RabbitMQ Connected");
    });
}
connectRabbitMQ();

// WebSocket Server
const wss = new WebSocketServer({ port: 8080 });

wss.on("connection", (ws) => {
    console.log("WebSocket Client Connected");

    ws.on("message", async (message) => {
        console.log(` Received from WebSocket: ${message}`);

        try {
            // Send message to Kafka
            await kafkaProducer.send({
                topic: "dbUpdates",
                messages: [{ value: message }],
            });

            // Send message to RabbitMQ
            if (rabbitMQChannel) {
                rabbitMQChannel.sendToQueue("dbUpdates", Buffer.from(message));
            }

            console.log(" Message sent to Kafka & RabbitMQ");
        } catch (error) {
            console.error("Error processing WebSocket message:", error);
        }
    });

    ws.on("close", () => console.log("Client disconnected"));
    ws.on("error", (error) => console.error(" WebSocket Error:", error));
});

// Kafka Consumers
async function consumeKafkaMessages() {
    await dbConsumer.connect();
    await dbConsumer.subscribe({ topic: "dbUpdates", fromBeginning: true });

    await pubSubConsumer.connect();
    await pubSubConsumer.subscribe({ topic: "pubSubMessages", fromBeginning: true });

    console.log("Subscribed to Kafka topics");

    dbConsumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            try {
                const receivedText = JSON.parse(message.value.toString());
                console.log(`Kafka DB Consumer Received:`, receivedText);

                const geminiResponse = await getGeminiResponse(receivedText.message);
                console.log(`Gemini Response: ${geminiResponse}`);

                await messagesCollection.insertOne({
                    topic,
                    receivedText,
                    geminiResponse,
                    timestamp: new Date(),
                });

                wss.clients.forEach((client) => {
                    if (client.readyState === WebSocket.OPEN) {
                        client.send(JSON.stringify({ from: "Gemini", message: geminiResponse }));
                    }
                });
            } catch (error) {
                console.error("Error processing Kafka message:", error);
            }
        },
    });

    pubSubConsumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            try {
                const receivedText = JSON.parse(message.value.toString());
                console.log(`Kafka PubSub Consumer Received:`, receivedText);

                const geminiResponse = await getGeminiResponse(receivedText.message);

                await messagesCollection.insertOne({
                    topic,
                    receivedText,
                    geminiResponse,
                    timestamp: new Date(),
                });

                wss.clients.forEach((client) => {
                    if (client.readyState === WebSocket.OPEN) {
                        client.send(JSON.stringify({ from: "Gemini", message: geminiResponse }));
                    }
                });
            } catch (error) {
                console.error("Error processing Kafka PubSub message:", error);
            }
        },
    });
}

// RabbitMQ Consumers
async function consumeRabbitMQMessages() {
    if (!rabbitMQChannel) return;

    rabbitMQChannel.consume("dbUpdates", async (msg) => {
        if (msg !== null) {
            const receivedText = JSON.parse(msg.content.toString());
            console.log(`RabbitMQ DB Consumer Received:`, receivedText);

            const geminiResponse = await getGeminiResponse(receivedText.message);

            await messagesCollection.insertOne({
                topic: "dbUpdates",
                receivedText,
                geminiResponse,
                timestamp: new Date(),
            });

            wss.clients.forEach((client) => {
                if (client.readyState === WebSocket.OPEN) {
                    client.send(JSON.stringify({ from: "Gemini", message: geminiResponse }));
                }
            });

            rabbitMQChannel.ack(msg);
        }
    });

    console.log("RabbitMQ Consumers Listening...");
}

// Start Consumers
consumeKafkaMessages().catch(console.error);
setTimeout(consumeRabbitMQMessages, 5000); // Delay to ensure RabbitMQ connection

console.log("WebSocket Server running on ws://localhost:8080");

