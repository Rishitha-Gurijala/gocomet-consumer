const { mongoConnect } = require("./utility/mongoConnect.js");
const { establishRedis } = require("./utility/redisConnect.js");
const amqp = require("amqplib");
const { v4: uuidv4 } = require("uuid");
const dotenv = require("dotenv");
dotenv.config();
const RABBITMQ_URL = process.env.RABBITMQ_URL;
const queue = process.env.queue;
const IDEMPOTENCY_TTL = parseInt(process.env.IDEMPOTENCY_TTL || "86400", 10); // Default 24 hours
let client;

try {
  client = establishRedis();
} catch (error) {
  console.error("Failed to connect to Redis:", error);
  process.exit(1);
}

async function checkDuplicateMessage(messageId) {
  if (!client) {
    throw new Error("Redis client not available");
  }
  const exists = await client.exists(`message:${messageId}`);
  return exists === 1;
}

async function markMessageAsProcessed(messageId) {
  if (!client) {
    throw new Error("Redis client not available");
  }
  await client.set(`message:${messageId}`, "1", "EX", IDEMPOTENCY_TTL);
}

async function consumeMessage() {
  try {
    const connection = await amqp.connect(RABBITMQ_URL);
    const channel = await connection.createChannel();
    await channel.assertQueue(queue, { durable: true });
    channel.prefetch(1);
    console.log(`👂 Waiting for messages in ${queue}. Press CTRL+C to exit.`);
    channel.consume(queue, async (msg) => {
      if (msg !== null) {
        const messageId = msg.properties.messageId || `gen-${uuidv4()}`;
        const content = JSON.parse(msg.content.toString());

        // Check for duplicate message
        const isDuplicate = await checkDuplicateMessage(messageId);
        if (isDuplicate) {
          console.log(`Skipping duplicate message: ${messageId}`);
          channel.ack(msg);
          return;
        }

        console.log(
          `Processing message: ${messageId} for player: ${content.playerId}`,
        );

        try {
          // Mark as processed before starting to process
          await markMessageAsProcessed(messageId);

          const db = await mongoConnect();
          const { playerId, gameId, betAmount, winAmount } = content;

          await updatePlays(db, { ...content, messageId });
          await new Promise((resolve) => setTimeout(resolve, 1000));

          await updatePlayers(db, playerId, gameId, winAmount, betAmount, {
            ...content,
            messageId,
          });

          channel.ack(msg);
          console.log(`Successfully processed message: ${messageId}`);
        } catch (error) {
          console.error(`Error processing message ${messageId}:`, error);
          // Remove the message ID to allow retry
          if (client) {
            await client.del(`message:${messageId}`);
          }
          // Negative acknowledgment - requeue the message
          channel.nack(msg, false, true);
        }
      }
    });
    // Handle connection errors
    connection.on("error", (err) => {
      console.error("AMQP connection error:", err);
      // Consider implementing reconnection logic here
    });
  } catch (error) {
    console.error("Error in consumeMessage:", error);
    // Consider implementing retry logic for the entire consumer
  }
}

consumeMessage();

async function updatePlayers(db, playerId, gameId, winAmount, betAmount, body) {
  let playerDocExists = await db.collection("players").findOne({ playerId });
  let doc = {
    playerId: playerId,
  };
  if (!playerDocExists) {
    doc = {
      playerId: playerId,
      totalGames: gameId ? 1 : 0,
      totalWins: winAmount ? 1 : 0,
      totalBets: betAmount ? 1 : 0,
    };
    await db.collection("players").insertOne(doc);
  } else {
    let totalGames = playerDocExists.totalGames
      ? playerDocExists.totalGames
      : 0;
    let totalWins = playerDocExists.totalWins ? playerDocExists.totalWins : 0;
    let totalBets = playerDocExists.totalBets ? playerDocExists.totalBets : 1;
    doc = {
      playerId: playerId,
      totalGames: totalGames + 1,
      totalWins: winAmount ? totalWins + 1 : totalWins,
      totalBets: betAmount ? totalBets + 1 : totalBets,
    };
    await db.collection("players").updateOne({ playerId }, { $set: doc });
  }
  doc = JSON.stringify(doc);
  if (!client) {
    establishRedis();
  }
  await client.set(playerId, doc, { EX: 60 });
}

async function updatePlays(db, body) {
  body.createdAt = new Date();
  await db.collection("plays").insertOne(body);
}
