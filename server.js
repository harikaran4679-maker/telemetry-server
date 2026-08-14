// ============================================================
// TEAM RENEW TELEMETRY SERVER
//
// ESP32
//   ↓
// HiveMQ Cloud MQTT
//   ↓
// Node.js / Render
//   ├── Team Dashboard  → /
//   ├── Driver Dashboard → /driver
//   ├── Shared GPS Route
//   └── InfluxDB
//
// IMPORTANT:
// - Local development: http://localhost:4000
// - Render production: https://YOUR-SERVICE.onrender.com
//
// ESP32 continues publishing to HiveMQ.
// ESP32 does NOT connect directly to Render.
// ============================================================

require("dotenv").config();

const path = require("path");
const express = require("express");
const http = require("http");
const mqtt = require("mqtt");
const { Server } = require("socket.io");

const {
    InfluxDB,
    Point
} = require("@influxdata/influxdb-client");

// ============================================================
// EXPRESS
// ============================================================

const app = express();

// Trust Render's reverse proxy
app.set("trust proxy", 1);

// JSON support
app.use(express.json());

// ============================================================
// HTTP SERVER
// ============================================================

const server = http.createServer(app);

// ============================================================
// SOCKET.IO
// ============================================================

const io = new Server(server, {
    cors: {
        origin: "*",
        methods: ["GET", "POST"]
    }
});

// ============================================================
// PUBLIC FOLDER
// ============================================================

const PUBLIC_DIR = path.join(__dirname, "public");

app.use(express.static(PUBLIC_DIR));

// ============================================================
// SHARED GPS ROUTE
// ============================================================
//
// Both dashboards use this same server-side route.
//
// Team Dashboard ─────┐
//                     ├── routeHistory
// Driver Dashboard ───┘
//
// ============================================================

const routeHistory = [];

const MAX_ROUTE_POINTS = 5000;

// ============================================================
// TEAM DASHBOARD
// ============================================================

app.get("/", (req, res) => {

    res.sendFile(
        path.join(PUBLIC_DIR, "index.html")
    );

});

// ============================================================
// DRIVER DASHBOARD
// ============================================================

app.get("/driver", (req, res) => {

    res.sendFile(
        path.join(PUBLIC_DIR, "driver.html")
    );

});

// ============================================================
// HEALTH CHECK
// ============================================================

app.get("/health", (req, res) => {

    res.status(200).json({

        status: "ok",

        service: "Team Renew Telemetry Server",

        environment:
            process.env.NODE_ENV || "development",

        port: PORT,

        routePoints:
            routeHistory.length,

        mqttConnected:
            mqttClient.connected

    });

});

// ============================================================
// PORT
// ============================================================
//
// Render automatically provides process.env.PORT.
//
// Local:
// PORT=4000
//
// Render:
// PORT=<Render assigned port>
//
// ============================================================

const PORT =
    Number(process.env.PORT) || 4000;

// ============================================================
// PUBLIC URL
// ============================================================
//
// Optional.
//
// Add this on Render:
//
// PUBLIC_URL=https://your-service.onrender.com
//
// It is only used for console information.
// ============================================================

const PUBLIC_URL =
    process.env.PUBLIC_URL ||
    `http://localhost:${PORT}`;

// ============================================================
// CHECK ENVIRONMENT
// ============================================================

console.log("");
console.log("Checking configuration...");
console.log("");

const requiredVariables = [

    "INFLUX_URL",
    "INFLUX_TOKEN",
    "INFLUX_ORG",
    "INFLUX_BUCKET",

    "MQTT_HOST",
    "MQTT_USERNAME",
    "MQTT_PASSWORD",
    "MQTT_TOPIC"

];

let configurationOK = true;

for (const variable of requiredVariables) {

    if (!process.env[variable]) {

        configurationOK = false;

        console.error(
            `✗ ${variable} is missing`
        );

    } else {

        console.log(
            `✓ ${variable} configured`
        );

    }

}

console.log("");

if (!configurationOK) {

    console.error(
        "⚠ Some environment variables are missing."
    );

    console.error(
        "The server may not connect correctly."
    );

}

console.log(
    "Configuration check complete."
);

console.log("");

// ============================================================
// INFLUXDB
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
// INFLUXDB DEFAULT TAGS
// ============================================================

writeApi.useDefaultTags({

    source: "ESP32"

});

// ============================================================
// MQTT OPTIONS
// ============================================================

const mqttOptions = {

    host:
        process.env.MQTT_HOST,

    port:
        Number(process.env.MQTT_PORT) || 8883,

    protocol: "mqtts",

    username:
        process.env.MQTT_USERNAME,

    password:
        process.env.MQTT_PASSWORD,

    reconnectPeriod: 2000,

    connectTimeout: 10000,

    clean: true

};

// ============================================================
// MQTT CLIENT
// ============================================================

const mqttClient =
    mqtt.connect(mqttOptions);

// ============================================================
// SAFE NUMBER
// ============================================================

function numberOrNull(value) {

    const number = Number(value);

    if (
        Number.isFinite(number)
    ) {

        return number;

    }

    return null;

}

// ============================================================
// ADD FLOAT FIELD
// ============================================================

function addFloatField(
    point,
    fieldName,
    value
) {

    const number =
        numberOrNull(value);

    if (number !== null) {

        point.floatField(
            fieldName,
            number
        );

    }

}

// ============================================================
// ADD ROUTE POINT
// ============================================================

function addRoutePoint(
    lat,
    lon,
    alt
) {

    // --------------------------------------------------------
    // Validate GPS
    // --------------------------------------------------------

    if (
        lat === null ||
        lon === null
    ) {

        return;

    }

    // --------------------------------------------------------
    // Ignore zero coordinates
    // --------------------------------------------------------

    if (
        lat === 0 ||
        lon === 0
    ) {

        return;

    }

    // --------------------------------------------------------
    // Create route point
    // --------------------------------------------------------

    const routePoint = {

        lat: lat,

        lon: lon,

        alt:
            alt !== null
                ? alt
                : null,

        timestamp:
            Date.now()

    };

    // --------------------------------------------------------
    // Store route
    // --------------------------------------------------------

    routeHistory.push(
        routePoint
    );

    // --------------------------------------------------------
    // Limit memory
    // --------------------------------------------------------

    if (
        routeHistory.length >
        MAX_ROUTE_POINTS
    ) {

        routeHistory.shift();

    }

    // --------------------------------------------------------
    // Send ONLY new point
    // to ALL connected dashboards
    // --------------------------------------------------------

    io.emit(
        "routePoint",
        routePoint
    );

}

// ============================================================
// SERVER START
// ============================================================

server.listen(

    PORT,

    "0.0.0.0",

    () => {

        console.log("");
        console.log(
            "================================================"
        );
        console.log(
            "       TEAM RENEW TELEMETRY SERVER"
        );
        console.log(
            "================================================"
        );

        console.log(
            `Environment: ${
                process.env.NODE_ENV || "development"
            }`
        );

        console.log(
            `Port: ${PORT}`
        );

        console.log("");

        console.log(
            `Team Dashboard: ${PUBLIC_URL}/`
        );

        console.log(
            `Driver Dashboard: ${PUBLIC_URL}/driver`
        );

        console.log(
            `Health: ${PUBLIC_URL}/health`
        );

        console.log("");

        console.log(
            "MQTT: HiveMQ Cloud"
        );

        console.log(
            "Database: InfluxDB"
        );

        console.log(
            "================================================"
        );

        console.log("");

    }

);

// ============================================================
// MQTT CONNECT
// ============================================================

mqttClient.on(

    "connect",

    () => {

        console.log(
            "✓ Connected to HiveMQ Cloud"
        );

        const topic =
            process.env.MQTT_TOPIC;

        mqttClient.subscribe(

            topic,

            {
                qos: 0
            },

            (error) => {

                if (error) {

                    console.error(
                        "✗ MQTT Subscribe Error:"
                    );

                    console.error(
                        error.message
                    );

                    return;

                }

                console.log(
                    `✓ Subscribed to: ${topic}`
                );

            }

        );

    }

);

// ============================================================
// MQTT ERROR
// ============================================================

mqttClient.on(

    "error",

    (error) => {

        console.error("");

        console.error(
            "✗ MQTT Error:"
        );

        console.error(
            error.message
        );

        console.error("");

    }

);

// ============================================================
// MQTT RECONNECT
// ============================================================

mqttClient.on(

    "reconnect",

    () => {

        console.log(
            "↻ Reconnecting to HiveMQ..."
        );

    }

);

// ============================================================
// MQTT OFFLINE
// ============================================================

mqttClient.on(

    "offline",

    () => {

        console.log(
            "✗ MQTT Offline"
        );

    }

);

// ============================================================
// MQTT CLOSE
// ============================================================

mqttClient.on(

    "close",

    () => {

        console.log(
            "MQTT connection closed"
        );

    }

);

// ============================================================
// MQTT MESSAGE
// ============================================================

mqttClient.on(

    "message",

    async (topic, message) => {

        try {

            // ==================================================
            // PARSE JSON
            // ==================================================

            const data =
                JSON.parse(
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

            // ==================================================
            // SEND TELEMETRY TO ALL DASHBOARDS
            // ==================================================

            io.emit(
                "telemetry",
                data
            );

            console.log(
                "✓ Telemetry sent to dashboard"
            );

            // ==================================================
            // CREATE INFLUXDB POINT
            // ==================================================

            const point =
                new Point(
                    "telemetry"
                );

            // ==================================================
            // VEHICLE TAG
            // ==================================================

            if (
                data.vehicle !== undefined &&
                data.vehicle !== null
            ) {

                point.tag(
                    "vehicle",
                    String(data.vehicle)
                );

            }

            // ==================================================
            // VEHICLE DATA
            // ==================================================

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

            // ==================================================
            // FUEL CELL
            // ==================================================

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

                // ------------------------------------------------
                // ERROR FLAG
                // ------------------------------------------------

                const fcError =
                    numberOrNull(
                        data.fc.error_flag
                    );

                if (
                    fcError !== null
                ) {

                    point.intField(
                        "fc_error_flag",
                        Math.trunc(fcError)
                    );

                }

            }

            // ==================================================
            // GPS
            // ==================================================

            if (
                data.gps !== undefined &&
                data.gps !== null
            ) {

                const lat =
                    numberOrNull(
                        data.gps.lat
                    );

                const lon =
                    numberOrNull(
                        data.gps.lon
                    );

                const alt =
                    numberOrNull(
                        data.gps.alt
                    );

                // ------------------------------------------------
                // InfluxDB GPS
                // ------------------------------------------------

                if (
                    lat !== null
                ) {

                    point.floatField(
                        "gps_lat",
                        lat
                    );

                }

                if (
                    lon !== null
                ) {

                    point.floatField(
                        "gps_lon",
                        lon
                    );

                }

                if (
                    alt !== null
                ) {

                    point.floatField(
                        "gps_alt",
                        alt
                    );

                }

                // ------------------------------------------------
                // Shared route
                // ------------------------------------------------

                addRoutePoint(
                    lat,
                    lon,
                    alt
                );

            }

            // ==================================================
            // CONNECTION STATUS
            // ==================================================

            if (
                data.connected !== undefined
            ) {

                point.intField(
                    "connected",
                    data.connected ? 1 : 0
                );

            }

            // ==================================================
            // WRITE TO INFLUXDB
            // ==================================================

            writeApi.writePoint(
                point
            );

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

        // ====================================================
        // SEND COMPLETE EXISTING ROUTE
        // ====================================================

        socket.emit(
            "routeHistory",
            routeHistory
        );

        console.log(
            `✓ Sent ${routeHistory.length} GPS points to dashboard`
        );

        // ====================================================
        // REQUEST ROUTE HISTORY
        // ====================================================

        socket.on(

            "requestRouteHistory",

            () => {

                socket.emit(
                    "routeHistory",
                    routeHistory
                );

            }

        );

        // ====================================================
        // DISCONNECT
        // ====================================================

        socket.on(

            "disconnect",

            (reason) => {

                console.log(
                    `✗ Dashboard disconnected: ${socket.id} (${reason})`
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

    if (
        shuttingDown
    ) {

        return;

    }

    shuttingDown = true;

    console.log("");

    console.log(
        "Shutting down telemetry server..."
    );

    // ========================================================
    // STOP ACCEPTING NEW CONNECTIONS
    // ========================================================

    server.close(
        () => {
            console.log(
                "✓ HTTP server closed"
            );
        }
    );

    // ========================================================
    // MQTT
    // ========================================================

    try {

        await new Promise(
            (resolve) => {

                if (
                    !mqttClient.connected
                ) {

                    resolve();

                    return;

                }

                mqttClient.end(
                    false,
                    {},
                    () => {

                        console.log(
                            "✓ MQTT connection closed"
                        );

                        resolve();

                    }
                );

            }
        );

    }

    catch (error) {

        console.error(
            "MQTT shutdown error:",
            error.message
        );

    }

    // ========================================================
    // INFLUXDB
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
            error.message
        );

    }

    console.log(
        "✓ Telemetry server shutdown complete"
    );

    process.exit(0);

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

// ============================================================
// UNHANDLED ERRORS
// ============================================================

process.on(
    "uncaughtException",
    (error) => {

        console.error(
            "✗ Uncaught Exception:",
            error
        );

    }
);

process.on(
    "unhandledRejection",
    (error) => {

        console.error(
            "✗ Unhandled Promise Rejection:",
            error
        );

    }
);