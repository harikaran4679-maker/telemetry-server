require("dotenv").config();

const { InfluxDB, Point } = require("@influxdata/influxdb-client");
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

// ---------------- InfluxDB ----------------

const influxDB = new InfluxDB({
  url: process.env.INFLUX_URL,
  token: process.env.INFLUX_TOKEN
});

const writeApi = influxDB.getWriteApi(
  process.env.INFLUX_ORG,
  process.env.INFLUX_BUCKET
);

writeApi.useDefaultTags({
  source: "ESP32"
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

        const point = new Point("imu");

        if (data.ax !== undefined) point.floatField("ax", data.ax);
        if (data.ay !== undefined) point.floatField("ay", data.ay);
        if (data.az !== undefined) point.floatField("az", data.az);

        if (data.gx !== undefined) point.floatField("gx", data.gx);
        if (data.gy !== undefined) point.floatField("gy", data.gy);
        if (data.gz !== undefined) point.floatField("gz", data.gz);

        if (data.temperature !== undefined)
            point.floatField("temperature", data.temperature);

        writeApi.writePoint(point);

    } catch (err) {
        console.log(err);
    }

});

process.on("SIGINT", async () => {
    await writeApi.close();
    process.exit();
});