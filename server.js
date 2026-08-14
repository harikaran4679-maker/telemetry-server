// ============================================================
// TEAM RENEW TELEMETRY SERVER
// ESP32 → HiveMQ → Node.js → Dashboard + InfluxDB
// ============================================================

require("dotenv").config();

const express = require("express");
const http = require("http");
const path = require("path");
const mqtt = require("mqtt");
const { Server } = require("socket.io");

const {
    InfluxDB,
    Point
} = require("@influxdata/influxdb-client");


// ============================================================
// EXPRESS + SOCKET.IO
// ============================================================

const app = express();

const server = http.createServer(app);

const io = new Server(server, {
    cors: {
        origin: "*"
    }
});


// ============================================================
// SERVE DASHBOARDS
// ============================================================

app.use(express.static(path.join(__dirname, "public")));


// ============================================================
// TEAM DASHBOARD
// http://localhost:4000/
// ============================================================

app.get("/", (req, res) => {

    res.sendFile(
        path.join(__dirname, "public", "index.html")
    );

});


// ============================================================
// DRIVER DASHBOARD
// http://localhost:4000/driver
// ============================================================

app.get("/driver", (req, res) => {

    res.sendFile(
        path.join(__dirname, "public", "driver.html")
    );

});


// ============================================================
// SERVER PORT
// ============================================================

const PORT = Number(process.env.PORT) || 4000;


// ============================================================
// CHECK ENVIRONMENT VARIABLES
// ============================================================

console.log("");
console.log("Checking configuration...");


if (!process.env.INFLUX_URL) {

    console.error("✗ INFLUX_URL is missing");

}

if (!process.env.INFLUX_TOKEN) {

    console.error("✗ INFLUX_TOKEN is missing");

}

if (!process.env.INFLUX_ORG) {

    console.error("✗ INFLUX_ORG is missing");

}

if (!process.env.INFLUX_BUCKET) {

    console.error("✗ INFLUX_BUCKET is missing");

}

if (!process.env.MQTT_HOST) {

    console.error("✗ MQTT_HOST is missing");

}

if (!process.env.MQTT_USERNAME) {

    console.error("✗ MQTT_USERNAME is missing");

}

if (!process.env.MQTT_PASSWORD) {

    console.error("✗ MQTT_PASSWORD is missing");

}

if (!process.env.MQTT_TOPIC) {

    console.error("✗ MQTT_TOPIC is missing");

}


console.log("Configuration check complete.");
console.log("");


// ============================================================
// INFLUXDB CONFIGURATION
// ============================================================

const influxDB = new InfluxDB({

    url: process.env.INFLUX_URL,

    token: process.env.INFLUX_TOKEN

});


// ============================================================
// INFLUXDB WRITE API
// ============================================================

const writeApi = influxDB.getWriteApi(

    process.env.INFLUX_ORG,

    process.env.INFLUX_BUCKET,

    "ms",

    {
        batchSize: 10,
        flushInterval: 1000
    }

);


// ============================================================
// DEFAULT INFLUX TAG
// ============================================================

writeApi.useDefaultTags({

    source: "ESP32"

});


// ============================================================
// MQTT CONFIGURATION
// ============================================================

const mqttClient = mqtt.connect({

    host: process.env.MQTT_HOST,

    port: Number(process.env.MQTT_PORT) || 8883,

    protocol: "mqtts",

    username: process.env.MQTT_USERNAME,

    password: process.env.MQTT_PASSWORD,

    reconnectPeriod: 2000,

    connectTimeout: 10000,

    clean: true

});


// ============================================================
// SERVER START
// ============================================================

server.listen(PORT, () => {

    console.log("");
    console.log("==============================================");
    console.log("       TEAM RENEW TELEMETRY SERVER");
    console.log("==============================================");

    console.log(
        `Server running on port ${PORT}`
    );

    console.log(
        `Team Dashboard: http://localhost:${PORT}`
    );

    console.log(
        `Driver Dashboard: http://localhost:${PORT}/driver`
    );

    console.log("==============================================");
    console.log("");

});


// ============================================================
// MQTT CONNECT
// ============================================================

mqttClient.on("connect", () => {

    console.log("✓ Connected to HiveMQ Cloud");

    const topic = process.env.MQTT_TOPIC;


    mqttClient.subscribe(

        topic,

        (error) => {

            if (error) {

                console.error(
                    "✗ MQTT Subscribe Error:"
                );

                console.error(error);

            }

            else {

                console.log(
                    `✓ Subscribed to: ${topic}`
                );

            }

        }

    );

});


// ============================================================
// MQTT ERROR
// ============================================================

mqttClient.on("error", (error) => {

    console.error("");

    console.error("✗ MQTT Error:");

    console.error(error);

    console.error("");

});


// ============================================================
// MQTT RECONNECT
// ============================================================

mqttClient.on("reconnect", () => {

    console.log(
        "↻ Reconnecting to HiveMQ..."
    );

});


// ============================================================
// MQTT OFFLINE
// ============================================================

mqttClient.on("offline", () => {

    console.log(
        "✗ MQTT Offline"
    );

});


// ============================================================
// MQTT CLOSE
// ============================================================

mqttClient.on("close", () => {

    console.log(
        "MQTT connection closed"
    );

});


// ============================================================
// HELPER: SAFE NUMBER
// ============================================================

function numberOrNull(value) {

    const number = Number(value);

    if (Number.isFinite(number)) {

        return number;

    }

    return null;

}


// ============================================================
// HELPER: ADD FLOAT FIELD
// ============================================================

function addFloatField(

    point,
    fieldName,
    value

) {

    const number = numberOrNull(value);

    if (number !== null) {

        point.floatField(

            fieldName,
            number

        );

    }

}


// ============================================================
// RECEIVE MQTT MESSAGE
// ============================================================

mqttClient.on(

    "message",

    (topic, message) => {

        try {

            // =================================================
            // PARSE JSON
            // =================================================

            const data = JSON.parse(
                message.toString()
            );


            console.log("");

            console.log(
                "========== MQTT TELEMETRY =========="
            );


            console.dir(

                data,

                {
                    depth: null,
                    colors: true
                }

            );


            // =================================================
            // SEND TELEMETRY TO ALL DASHBOARDS
            // =================================================
            //
            // IMPORTANT:
            // The received JSON is stored in "data".
            //
            // DO NOT use:
            //
            // io.emit("telemetry", telemetry);
            //
            // because "telemetry" does not exist.
            //
            // =================================================

            io.emit(

                "telemetry",

                data

            );


            console.log(
                "✓ Telemetry sent to dashboard"
            );


            // =================================================
            // CREATE INFLUXDB POINT
            // =================================================

            const point = new Point(
                "telemetry"
            );


            // =================================================
            // VEHICLE TAG
            // =================================================

            if (

                data.vehicle !== undefined &&

                data.vehicle !== null

            ) {

                point.tag(

                    "vehicle",

                    String(data.vehicle)

                );

            }


            // =================================================
            // VEHICLE / MOTOR DATA
            // =================================================

            addFloatField(

                point,
                "throttle",
                data.throttle

            );


            addFloatField(

                point,
                "accel",
                data.accel

            );


            addFloatField(

                point,
                "decel",
                data.decel

            );


            addFloatField(

                point,
                "slope",
                data.slope

            );


            addFloatField(

                point,
                "ds18b20_temp",
                data.ds18b20_temp

            );


            addFloatField(

                point,
                "bus_voltage",
                data.bus_voltage

            );


            addFloatField(

                point,
                "motor_current",
                data.motor_current

            );


            addFloatField(

                point,
                "m_rpm",
                data.m_rpm

            );


            addFloatField(

                point,
                "m_power",
                data.m_power

            );


            addFloatField(

                point,
                "speed_kmh",
                data.speed_kmh

            );


            addFloatField(

                point,
                "distance_m",
                data.distance_m

            );


            // =================================================
            // FUEL CELL DATA
            // =================================================

            if (

                data.fc !== undefined &&

                data.fc !== null

            ) {

                addFloatField(

                    point,
                    "fc_voltage",
                    data.fc.voltage

                );


                addFloatField(

                    point,
                    "fc_current",
                    data.fc.current

                );


                addFloatField(

                    point,
                    "fc_power",
                    data.fc.power

                );


                addFloatField(

                    point,
                    "fc_stack_temp",
                    data.fc.stack_temp

                );


                addFloatField(

                    point,
                    "fc_h2_leak_volts",
                    data.fc.h2_leak_volts

                );


                addFloatField(

                    point,
                    "fc_env_temp",
                    data.fc.env_temp

                );


                addFloatField(

                    point,
                    "fc_batt_voltage",
                    data.fc.batt_voltage

                );


                addFloatField(

                    point,
                    "fc_batt_current",
                    data.fc.batt_current

                );


                // =============================================
                // FUEL CELL ERROR FLAG
                // =============================================

                const fcError =
                    numberOrNull(
                        data.fc.error_flag
                    );


                if (fcError !== null) {

                    point.intField(

                        "fc_error_flag",

                        Math.trunc(fcError)

                    );

                }

            }


            // =================================================
            // GPS DATA
            // =================================================

            if (

                data.gps !== undefined &&

                data.gps !== null

            ) {

                addFloatField(

                    point,
                    "gps_lat",
                    data.gps.lat

                );


                addFloatField(

                    point,
                    "gps_lon",
                    data.gps.lon

                );


                addFloatField(

                    point,
                    "gps_alt",
                    data.gps.alt

                );

            }


            // =================================================
            // CONNECTION STATUS
            // =================================================

            if (

                data.connected !== undefined

            ) {

                point.intField(

                    "connected",

                    data.connected ? 1 : 0

                );

            }


            // =================================================
            // WRITE TO INFLUXDB
            // =================================================

            writeApi.writePoint(point);


            console.log(
                "✓ Telemetry queued for InfluxDB"
            );


        }

        catch (error) {

            console.error("");

            console.error(
                "✗ MQTT / JSON / InfluxDB Error"
            );

            console.error(
                error.message
            );

            console.error("");

        }

    }

);


// ============================================================
// SOCKET.IO CONNECTION
// ============================================================

io.on(

    "connection",

    (socket) => {

        console.log(
            `✓ Dashboard connected: ${socket.id}`
        );


        socket.on(

            "disconnect",

            () => {

                console.log(
                    `✗ Dashboard disconnected: ${socket.id}`
                );

            }

        );

    }

);


// ============================================================
// GRACEFUL SHUTDOWN
// ============================================================

let shuttingDown = false;


async function shutdown() {

    if (shuttingDown) {

        return;

    }


    shuttingDown = true;


    console.log("");

    console.log(
        "Shutting down telemetry server..."
    );


    // ========================================================
    // STOP MQTT
    // ========================================================

    try {

        mqttClient.end(

            true,

            () => {

                console.log(
                    "✓ MQTT connection closed"
                );

            }

        );

    }

    catch (error) {

        console.error(
            "MQTT shutdown error:",
            error
        );

    }


    // ========================================================
    // FLUSH + CLOSE INFLUXDB
    // ========================================================

    try {

        await writeApi.close();


        console.log(
            "✓ InfluxDB connection closed"
        );

    }

    catch (error) {

        console.error(
            "InfluxDB shutdown error:",
            error
        );

    }


    // ========================================================
    // CLOSE SERVER
    // ========================================================

    server.close(

        () => {

            console.log(
                "✓ Server closed"
            );

            process.exit(0);

        }

    );

}


// ============================================================
// PROCESS SIGNALS
// ============================================================

process.on(

    "SIGINT",

    shutdown

);


process.on(

    "SIGTERM",

    shutdown

);