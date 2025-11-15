let { createClient } = require("redis");
const dotenv = require("dotenv");
dotenv.config();

function establishRedis() {
  global.client = createClient();
  client.on("error", (err) => console.log("Redis Client Error", err));
  client.on("ready", () => {
    console.log("Redis is ready Test 5");
  });

  client.on("reconnecting", () => {
    console.log("client is reconnecting");
  });

  client.on("end", () => {
    console.log("Redis connection ended");
  });

  client.on("connect", () => {
    global.console.log("connected");
  });

  client.on("error", (err) => {
    console.error("Redis error: ", err);
  });
  client.connect();
  console.log("Connected to Redis!!!!");
}

module.exports = { establishRedis };
