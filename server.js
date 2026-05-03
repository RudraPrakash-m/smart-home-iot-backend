const express = require("express");
const mqtt = require("mqtt");
const cors = require("cors");
const mongoose = require("mongoose");
const http = require("http");
const { Server } = require("socket.io");
require("dotenv").config();

const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*" },
});

const PORT = 5000;

// ================= MONGODB =================
const MONGO_URI = process.env.CONNECTION_STRING;

mongoose
  .connect(MONGO_URI)
  .then(() => console.log("✅ MongoDB Connected (Atlas)"))
  .catch((err) => {
    console.error("❌ DB Error:", err.message);
    process.exit(1);
  });

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
initState();

// ================= MQTT =================
const client = mqtt.connect("mqtt://broker.hivemq.com");

const CONTROL_TOPIC = "rudra_home/control";
const ALERT_TOPIC = "rudra_home/alert";
const ONLINE_TOPIC = "rudra_home/online";

client.on("connect", () => {
  console.log("✅ MQTT Connected");

  client.subscribe(ALERT_TOPIC);
  client.subscribe(ONLINE_TOPIC);
});

// ================= MQTT RECEIVE =================
client.on("message", async (topic, message) => {
  const msg = message.toString();

  console.log("📡 MQTT:", topic, msg);

  // ===== ALERTS =====
  if (topic === ALERT_TOPIC) {
    let text = "";

    if (msg === "INTRUSION") text = "Intrusion detected";
    else if (msg === "FIRE") text = "Fire detected";
    else if (msg === "GAS") text = "Gas leak detected";
    else if (msg === "RAIN") text = "Rain detected";
    else text = "Unknown alert";

    // store alert
    const alert = await Alert.create({
      type: msg,
      message: text,
    });

    // turn buzzer ON
    await State.updateOne({}, { buzzer: true });
    client.publish(CONTROL_TOPIC, "BUZZER_ON");

    // send to frontend
    io.emit("alert", alert);
  }

  // ===== DEVICE STATUS =====
  if (topic === ONLINE_TOPIC) {
    io.emit("device", msg);
  }
});

// ================= API =================

// HEALTH
app.get("/", (req, res) => {
  res.send("Backend running");
});

// GET STATE
app.get("/state", async (req, res) => {
  const state = await State.findOne();
  res.json(state);
});

// GET ALERTS
app.get("/alerts", async (req, res) => {
  const alerts = await Alert.find().sort({ createdAt: -1 });
  res.json(alerts);
});

// LOCK
app.post("/lock", async (req, res) => {
  await State.updateOne({}, { homeLock: true });
  client.publish(CONTROL_TOPIC, "LOCK", { retain: true });
  res.json({ success: true });
});

// UNLOCK
app.post("/unlock", async (req, res) => {
  await State.updateOne({}, { homeLock: false });
  client.publish(CONTROL_TOPIC, "UNLOCK", { retain: true });
  res.json({ success: true });
});

// LIGHT ON
app.post("/light/on", async (req, res) => {
  await State.updateOne({}, { insideLight: true });
  client.publish(CONTROL_TOPIC, "LIGHT_ON");
  res.json({ success: true });
});

// LIGHT OFF
app.post("/light/off", async (req, res) => {
  await State.updateOne({}, { insideLight: false });
  client.publish(CONTROL_TOPIC, "LIGHT_OFF");
  res.json({ success: true });
});

// 🔕 BUZZER OFF
app.post("/buzzer/off", async (req, res) => {
  await State.updateOne({}, { buzzer: false });

  // mark all alerts resolved
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

// ================= START =================
server.listen(PORT, () => {
  console.log(`🚀 Server running on http://localhost:${PORT}`);
});
