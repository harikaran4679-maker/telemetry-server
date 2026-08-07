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

        // Send to Website
        io.emit("telemetry", data);

        // Create InfluxDB Point
        const point = new Point("imu");

        if (data.ax !== undefined)
            point.floatField("ax", Number(data.ax));

        if (data.ay !== undefined)
            point.floatField("ay", Number(data.ay));

        if (data.az !== undefined)
            point.floatField("az", Number(data.az));

        if (data.gx !== undefined)
            point.floatField("gx", Number(data.gx));

        if (data.gy !== undefined)
            point.floatField("gy", Number(data.gy));

        if (data.gz !== undefined)
            point.floatField("gz", Number(data.gz));

        if (data.temperature !== undefined)
            point.floatField("temperature", Number(data.temperature));

        // Write to InfluxDB
        writeApi.writePoint(point);

        // Force write immediately (good for testing)
        await writeApi.flush();

        console.log("✓ Data written to InfluxDB");

    } catch (err) {
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