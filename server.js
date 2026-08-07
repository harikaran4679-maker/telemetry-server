const mqtt = require("mqtt");
const express = require("express");
const http = require("http");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static("public"));

server.listen(4000, () => {
    console.log("Server running on http://localhost:4000");
});

// ---------------- MQTT ----------------

const client = mqtt.connect({
    host: "dd225fb78fdb4985a16165763ef3296b.s1.eu.hivemq.cloud",
    port: 8883,
    protocol: "mqtts",
    username: "vehicledemo",
    password: "Harikaran@2006",
    reconnectPeriod: 1000
});

client.on("connect", () => {
    console.log("Connected to HiveMQ");

    client.subscribe("vehicles/VH-102/telemetry");
});

client.on("message", (topic, message) => {

    const data = JSON.parse(message.toString());

    console.clear();
    console.log(data);

    io.emit("telemetry", data);

});