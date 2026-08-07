require("dotenv").config();

const mqtt = require("mqtt");
const express = require("express");
const http = require("http");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static("public"));

// Render automatically provides the PORT
const PORT = process.env.PORT || 4000;

server.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});

// ---------------- MQTT ----------------

const client = mqtt.connect({
    host: process.env.MQTT_HOST,
    port: Number(process.env.MQTT_PORT),
    protocol: "mqtts",
    username: process.env.MQTT_USERNAME,
    password: process.env.MQTT_PASSWORD,
    reconnectPeriod: 1000
});

client.on("connect", () => {
    console.log("Connected to HiveMQ");
    client.subscribe(process.env.MQTT_TOPIC);
});

client.on("message", (topic, message) => {
    try {
        const data = JSON.parse(message.toString());

        console.clear();
        console.log(data);

        io.emit("telemetry", data);

    } catch (err) {
        console.log(err);
    }
});