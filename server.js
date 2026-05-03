require("dotenv").config();

const express = require("express");
const mqtt = require("mqtt");
const cors = require("cors");
const mongoose = require("mongoose");
const http = require("http");
const { Server } = require("socket.io");

const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*" },
});

const PORT = process.env.PORT || 5000;

// ================= MODELS =================
const Alert = mongoose.model("Alert", {
  type: String,
  message: String,
  resolved: { type: Boolean, default: false },
  createdAt: { type: Date, default: Date.now },
});

const State = mongoose.model("State", {
  homeLock: Boolean,
  insideLight: Boolean,
  buzzer: Boolean,
});

// ================= INIT STATE =================
async function initState() {
  const s = await State.findOne();
  if (!s) {
    await State.create({
      homeLock: false,
      insideLight: false,
      buzzer: false,
    });
  }
}

// ================= MQTT =================
const client = mqtt.connect("mqtt://broker.hivemq.com");

const CONTROL_TOPIC = "rudra_home/control";
const ALERT_TOPIC = "rudra_home/alert";

client.on("connect", () => {
  console.log("✅ MQTT Connected");
  client.subscribe(ALERT_TOPIC);
});

// ================= MQTT RECEIVE =================
client.on("message", async (topic, message) => {
  try {
    const msg = message.toString();

    console.log("📡 MQTT:", topic, msg);

    if (topic === ALERT_TOPIC) {
      let text = "";

      if (msg === "INTRUSION") text = "Intrusion detected";
      else if (msg === "FIRE") text = "Fire detected";
      else if (msg === "GAS") text = "Gas leak detected";
      else if (msg === "RAIN") text = "Rain detected";
      else text = "Unknown alert";

      const alert = await Alert.create({
        type: msg,
        message: text,
      });

      await State.updateOne({}, { buzzer: true });
      client.publish(CONTROL_TOPIC, "BUZZER_ON");

      io.emit("alert", alert);
    }
  } catch (err) {
    console.error("❌ MQTT Handler Error:", err.message);
  }
});

// ================= API =================

app.get("/", (req, res) => {
  res.send("Backend running");
});

app.get("/state", async (req, res) => {
  const state = await State.findOne();
  res.json(state);
});

app.get("/alerts", async (req, res) => {
  const alerts = await Alert.find().sort({ createdAt: -1 });
  res.json(alerts);
});

app.post("/lock", async (req, res) => {
  await State.updateOne({}, { homeLock: true });
  client.publish(CONTROL_TOPIC, "LOCK", { retain: true });
  res.json({ success: true });
});

app.post("/unlock", async (req, res) => {
  await State.updateOne({}, { homeLock: false });
  client.publish(CONTROL_TOPIC, "UNLOCK", { retain: true });
  res.json({ success: true });
});

app.post("/light/on", async (req, res) => {
  await State.updateOne({}, { insideLight: true });
  client.publish(CONTROL_TOPIC, "LIGHT_ON");
  res.json({ success: true });
});

app.post("/light/off", async (req, res) => {
  await State.updateOne({}, { insideLight: false });
  client.publish(CONTROL_TOPIC, "LIGHT_OFF");
  res.json({ success: true });
});

app.post("/buzzer/off", async (req, res) => {
  await State.updateOne({}, { buzzer: false });

  await Alert.updateMany({ resolved: false }, { resolved: true });

  client.publish(CONTROL_TOPIC, "BUZZER_OFF");

  res.json({ success: true });
});

// ================= SOCKET =================
io.on("connection", async (socket) => {
  console.log("🟢 Client connected");

  const state = await State.findOne();
  socket.emit("init", state);

  socket.on("disconnect", () => {
    console.log("🔴 Client disconnected");
  });
});

// ================= START SERVER AFTER DB =================
const startServer = async () => {
  try {
    await mongoose.connect(process.env.CONNECTION_STRING);
    console.log("✅ MongoDB Connected");

    await initState();

    server.listen(PORT, () => {
      console.log(`🚀 Server running on port ${PORT}`);
    });

  } catch (err) {
    console.error("❌ Startup Error:", err.message);
    process.exit(1);
  }
};

startServer();