require("dotenv").config();

const { InfluxDB, Point } = require("@influxdata/influxdb-client");
const mqtt = require("mqtt");
const express = require("express");
const http = require("http");
const { Server } = require("socket.io");

// ---------------- Express ----------------

const app = express();
const server = http.createServer(app);
const io = new Server(server);

app.use(express.static("public"));

// ---------------- Server ----------------

const PORT = process.env.PORT || 4000;

server.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});

// ---------------- InfluxDB ----------------

const influxDB = new InfluxDB({
    url: process.env.INFLUX_URL,
    token: process.env.INFLUX_TOKEN,
});

const writeApi = influxDB.getWriteApi(
    process.env.INFLUX_ORG,
    process.env.INFLUX_BUCKET
);

writeApi.useDefaultTags({
    source: "ESP32",
});

// ---------------- MQTT ----------------

const client = mqtt.connect({
    host: process.env.MQTT_HOST,
    port: Number(process.env.MQTT_PORT),
    protocol: "mqtts",
    username: process.env.MQTT_USERNAME,
    password: process.env.MQTT_PASSWORD,
    reconnectPeriod: 1000,
});

client.on("connect", () => {
    console.log("Connected to HiveMQ");

    client.subscribe(process.env.MQTT_TOPIC, (err) => {
        if (err) {
            console.error("MQTT Subscribe Error:", err);
        } else {
            console.log("Subscribed to:", process.env.MQTT_TOPIC);
        }
    });
});

// ---------------- Receive MQTT Data ----------------

client.on("message", async (topic, message) => {
    try {

        const data = JSON.parse(message.toString());

        console.clear();
        console.log(data);

        // Send to Dashboard
        io.emit("telemetry", data);

        // Create GPS Measurement
        const point = new Point("gps");

        // Tag
        if (data.vehicle)
            point.tag("vehicle", data.vehicle);

        // Fields
        if (data.lat !== undefined)
            point.floatField("lat", Number(data.lat));

        if (data.lon !== undefined)
            point.floatField("lon", Number(data.lon));

        if (data.altitude !== undefined)
            point.floatField("altitude", Number(data.altitude));

        if (data.speed !== undefined)
            point.floatField("speed", Number(data.speed));

        // Write to InfluxDB
        writeApi.writePoint(point);

        await writeApi.flush();

        console.log("✓ GPS written to InfluxDB");

    }
    catch (err) {

        console.error("InfluxDB/MQTT Error:");
        console.error(err);

    }
});

// ---------------- Shutdown ----------------

process.on("SIGINT", async () => {
    try {
        await writeApi.close();
        console.log("InfluxDB connection closed.");
    } catch (err) {
        console.error(err);
    }

    process.exit();
});