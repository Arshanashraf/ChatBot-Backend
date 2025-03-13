import { WebSocketServer, WebSocket } from "ws";
import { Kafka } from "kafkajs";
import { GoogleGenerativeAI } from "@google/generative-ai";
import { MongoClient } from "mongodb";
import dotenv from "dotenv";

dotenv.config();

const mongoClient = new MongoClient(process.env.MONGO_URI);
const dbName = "chatDB";
const collectionName = "messages";
let db, messagesCollection;

async function connectToMongoDB() {
    try {
        await mongoClient.connect();
        db = mongoClient.db(dbName);
        messagesCollection = db.collection(collectionName);
        console.log("Connected to MongoDB Atlas");
    } catch (error) {
        console.error("MongoDB Connection Error:", error);
    }
}
connectToMongoDB();

// Initialize Gemini AI
const genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);

async function getGeminiResponse(prompt) {
    try {
        const model = genAI.getGenerativeModel({ model: "gemini-1.5-pro" });
        const result = await model.generateContent(prompt);
        console.log("📡 Gemini API Raw Response:", result);

        // Extract and clean the response
        return cleanGeminiResponse(result.response.text());
    } catch (error) {
        console.error("❌ Gemini API Error:", error);
        return `Error: ${error.message || "Unknown error"}`;
    }
}

// Function to clean Gemini's response formatting
function cleanGeminiResponse(response) {
    return response.replace(/```json|```/g, "").trim(); // Remove code block formatting
}

// Kafka Configuration
const kafka = new Kafka({
    clientId: "websocket-server",
    brokers: ["localhost:9092"], 
});

const producer = kafka.producer();
const dbConsumer = kafka.consumer({ groupId: "dbConsumerGroupTest" });
const pubSubConsumer = kafka.consumer({ groupId: "pubSubConsumerGroup" });

async function startKafkaProducer() {
    try {
        await producer.connect();
        console.log("Kafka Producer Connected");
    } catch (error) {
        console.error("Kafka Producer Connection Error:", error);
    }
}
startKafkaProducer();

async function connectKafkaConsumer(consumer) {
    let retries = 5;
    while (retries > 0) {
        try {
            await consumer.connect();
            console.log("Kafka Consumer Connected");
            return;
        } catch (error) {
            console.error("Kafka Consumer Connection Error:", error);
            retries--;
            await new Promise((res) => setTimeout(res, 5000)); // Retry after 5 seconds
        }
    }
    console.error("Failed to connect Kafka consumer after retries");
}

// WebSocket Server
const wss = new WebSocketServer({ port: 8080 });

wss.on("connection", (ws) => {
    ws.on("message", async (message) => {
        console.log(`Received from WebSocket: ${message}`);

        await producer.send({
            topic: "dbUpdates",
            messages: [{ value: message }],
        });

        console.log("Message sent to Kafka");
    });

    ws.on("close", () => console.log("Client disconnected"));
});

// Kafka Consumers
async function consumeMessages() {
    await connectKafkaConsumer(dbConsumer);
    await connectKafkaConsumer(pubSubConsumer);

    console.log("🔄 Subscribing to Kafka topics...");
    await dbConsumer.subscribe({ topic: "dbUpdates", fromBeginning: true });
    await pubSubConsumer.subscribe({ topic: "pubSubMessages", fromBeginning: true });

    console.log("Subscribed successfully!");

    // Handle dbConsumer messages
    dbConsumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            try {
                const receivedText = JSON.parse(message.value.toString()); // Convert to object
                console.log(`📥 DB Consumer Received:`, receivedText);

                const geminiResponse = await getGeminiResponse(receivedText.message);
                console.log(`🤖 Gemini Response: ${geminiResponse}`);

                await messagesCollection.insertOne({
                    topic,
                    receivedText, // Now stored as an object
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

    // Handle pubSubConsumer messages
    pubSubConsumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            try {
                const receivedText = JSON.parse(message.value.toString());
                console.log(`PubSub Consumer Received:`, receivedText);

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
                console.error("Error processing PubSub message:", error);
            }
        },
    });
}

// Start Kafka Consumers
consumeMessages().catch(console.error);

// Graceful Shutdown
process.on("SIGINT", async () => {
    console.log(" Closing MongoDB and Kafka connections...");
    await mongoClient.close();
    await producer.disconnect();
    await dbConsumer.disconnect();
    await pubSubConsumer.disconnect();
    process.exit(0);
});

console.log("WebSocket server running on ws://localhost:8080");
