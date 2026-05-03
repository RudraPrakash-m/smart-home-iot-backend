const mqtt = require("mqtt");

const client = mqtt.connect("mqtt://broker.hivemq.com");

client.on("connect", () => {
  console.log("Connected");

  client.publish("rudra_home/alert", "RAIN");

  console.log("Alert sent: RAIN");

  setTimeout(() => client.end(), 1000);
});