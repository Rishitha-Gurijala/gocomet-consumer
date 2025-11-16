const { establishRedis } = require("./utility/redisConnect.js");
const amqp = require("amqplib");
const dotenv = require("dotenv");
dotenv.config();
const RABBITMQ_URL = process.env.RABBITMQ_URL;
const queue = process.env.queue;
let client;
const db = require("./mysqldb");
let sample_lat = 12.9719;
let sample_long = 77.5937;

try {
  client = establishRedis();
} catch (error) {
  console.error("Failed to connect to Redis:", error);
  process.exit(1);
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
        const messageId = msg.properties.messageId;
        const content = JSON.parse(msg.content.toString());
        try {
          const { userId, source, destination } = content;

          await createRide(db, { ...content, messageId });

          channel.ack(msg);
          console.log(`Successfully processed message: ${userId}`);
        } catch (error) {
          console.error(`Error processing message :`, error);
          channel.nack(msg, false, true);
        }
      }
    });
    connection.on("error", (err) => {
      console.error("AMQP connection error:", err);
    });
  } catch (error) {
    console.error("Error in consumeMessage:", error);
  }
}

async function createRide(db, body) {
  let id = generateFourDigitRandom();
  let userId = body.userId;
  let pickup_lat = body.source.latitude;
  let pickup_long = body.source.longitude;
  let dropoff_lat = body.destination.latitude;
  let dropoff_long = body.destination.longitude;
  const [result] = await db
    .promise()
    .query(
      "INSERT INTO rides (id, userId, pickup_lat, pickup_long, dropoff_lat, dropoff_long, status) VALUES (?, ?, ?, ?, ?, ?, ?)",
      [
        id,
        userId,
        pickup_lat,
        pickup_long,
        dropoff_lat,
        dropoff_long,
        "WAITING",
      ],
    );
  let query = `
      SELECT id,
      (
        6371 * ACOS(
          COS(RADIANS(?)) *
          COS(RADIANS(latitude)) *
          COS(RADIANS(longitude) - RADIANS(?)) +
          SIN(RADIANS(?)) *
          SIN(RADIANS(latitude))
        )
      ) AS distance
      FROM drivers
      WHERE is_available = "true"
      HAVING distance < 5
      ORDER BY distance ASC
    `;

  const [rows] = await db
    .promise()
    .query(query, [pickup_lat, pickup_long, pickup_lat]);
  let driverIds = rows.map((driver) => driver.id);
  userId = parseInt(userId);
  const [update] = await db
    .promise()
    .query(
      `UPDATE drivers SET users = JSON_ARRAY_APPEND(users, '$', ?) WHERE id IN (${driverIds})`,
      [userId],
    );
}

function generateFourDigitRandom() {
  return Math.floor(1000 + Math.random() * 9000);
}

consumeMessage();
