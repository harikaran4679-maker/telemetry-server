// ============================================================
// TEAM RENEW TELEMETRY SERVER
//
// ESP32
//   ↓
// HiveMQ Cloud MQTT
//   ↓
// Node.js / Render
//   ├── Team Dashboard      → /
//   ├── Driver Dashboard    → /driver
//   ├── Shared GPS Route
//   └── InfluxDB
//
// TELEMETRY FLOW
//
// ESP32
//   ↓
// HiveMQ
//   ↓
// Node.js
//   ↓
// ONE ENRICHED TELEMETRY PACKET
//   ├──→ Team Dashboard
//   ├──→ Driver Dashboard
//   └──→ InfluxDB
//
// IMPORTANT
//
// The same telemetry object contains:
//
// telemetry_id
// server_timestamp
// sensor values
//
// Team and Driver receive the SAME object.
// InfluxDB receives the SAME telemetry_id and
// server_timestamp.
//
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

app.set(
    "trust proxy",
    1
);

app.use(
    express.json()
);


// ============================================================
// HTTP SERVER
// ============================================================

const server =
    http.createServer(app);


// ============================================================
// SOCKET.IO
// ============================================================

const io =
    new Server(
        server,
        {
            cors: {
                origin: "*",
                methods: [
                    "GET",
                    "POST"
                ]
            }
        }
    );


// ============================================================
// PUBLIC FOLDER
// ============================================================

const PUBLIC_DIR =
    path.join(
        __dirname,
        "public"
    );

app.use(
    express.static(
        PUBLIC_DIR
    )
);


// ============================================================
// PORT
// ============================================================
//
// IMPORTANT FOR RENDER:
//
// Render provides PORT automatically.
//
// The server MUST listen on:
// 0.0.0.0
//
// ============================================================

const PORT =
    Number(
        process.env.PORT
    ) || 10000;


// ============================================================
// PUBLIC URL
// ============================================================
//
// Render automatically provides:
// RENDER_EXTERNAL_URL
//
// Example:
//
// https://team-renew-telemetry.onrender.com
//
// For local development:
//
// http://localhost:4000
//
// You can also manually set PUBLIC_URL.
//
// ============================================================

const PUBLIC_URL =
    process.env.PUBLIC_URL ||
    process.env.RENDER_EXTERNAL_URL ||
    `http://localhost:${PORT}`;


// ============================================================
// SHARED GPS ROUTE
// ============================================================

const routeHistory = [];

const MAX_ROUTE_POINTS = 5000;


// ============================================================
// TELEMETRY STATE
// ============================================================

let telemetrySequence = 0;

let lastTelemetry = null;

let lastTelemetryTime = null;

let totalTelemetryPackets = 0;

let invalidTelemetryPackets = 0;


// ============================================================
// INFLUXDB CLIENT
// ============================================================

let influxDB = null;

let writeApi = null;


// ============================================================
// MQTT CLIENT
// ============================================================

let mqttClient = null;


// ============================================================
// CONFIGURATION CHECK
// ============================================================

console.log("");

console.log(
    "================================================"
);

console.log(
    "Checking configuration..."
);

console.log(
    "================================================"
);

console.log("");


// ============================================================
// REQUIRED VARIABLES
// ============================================================

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


// ============================================================
// CHECK VARIABLES
// ============================================================

let configurationOK = true;

for (
    const variable
    of requiredVariables
) {

    if (
        !process.env[variable]
    ) {

        configurationOK =
            false;

        console.error(
            `✗ ${variable} is missing`
        );

    }

    else {

        console.log(
            `✓ ${variable} configured`
        );

    }

}

console.log("");


// ============================================================
// CONFIGURATION RESULT
// ============================================================

if (
    !configurationOK
) {

    console.error(
        "⚠ Some environment variables are missing."
    );

    console.error(
        "The server may not connect correctly."
    );

}

else {

    console.log(
        "✓ All required environment variables configured."
    );

}

console.log("");


// ============================================================
// INFLUXDB INITIALIZATION
// ============================================================

if (
    process.env.INFLUX_URL &&
    process.env.INFLUX_TOKEN &&
    process.env.INFLUX_ORG &&
    process.env.INFLUX_BUCKET
) {

    influxDB =
        new InfluxDB({

            url:
                process.env.INFLUX_URL,

            token:
                process.env.INFLUX_TOKEN

        });


    // ========================================================
    // INFLUX WRITE API
    // ========================================================

    writeApi =
        influxDB.getWriteApi(

            process.env.INFLUX_ORG,

            process.env.INFLUX_BUCKET,

            "ms",

            {

                batchSize: 10,

                flushInterval: 1000

            }

        );


    // ========================================================
    // DEFAULT TAGS
    // ========================================================

    writeApi.useDefaultTags({

        source:
            "ESP32"

    });


    console.log(
        "✓ InfluxDB initialized"
    );

}

else {

    console.error(
        "✗ InfluxDB not initialized because configuration is incomplete."
    );

}


// ============================================================
// MQTT OPTIONS
// ============================================================

const mqttOptions = {

    host:
        process.env.MQTT_HOST,

    port:
        Number(
            process.env.MQTT_PORT
        ) || 8883,

    protocol:
        "mqtts",

    username:
        process.env.MQTT_USERNAME,

    password:
        process.env.MQTT_PASSWORD,

    reconnectPeriod:
        2000,

    connectTimeout:
        10000,

    clean:
        true

};


// ============================================================
// MQTT CLIENT
// ============================================================

if (
    process.env.MQTT_HOST &&
    process.env.MQTT_USERNAME &&
    process.env.MQTT_PASSWORD &&
    process.env.MQTT_TOPIC
) {

    mqttClient =
        mqtt.connect(
            mqttOptions
        );

}

else {

    console.error(
        "✗ MQTT client not started because configuration is incomplete."
    );

}


// ============================================================
// SAFE NUMBER
// ============================================================

function numberOrNull(
    value
) {

    const number =
        Number(value);


    if (
        Number.isFinite(
            number
        )
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
        numberOrNull(
            value
        );


    if (
        number !== null
    ) {

        point.floatField(
            fieldName,
            number
        );

    }

}


// ============================================================
// ADD INTEGER FIELD
// ============================================================

function addIntegerField(
    point,
    fieldName,
    value
) {

    const number =
        numberOrNull(
            value
        );


    if (
        number !== null
    ) {

        point.intField(
            fieldName,
            Math.trunc(
                number
            )
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
    // Validate
    // --------------------------------------------------------

    if (
        lat === null ||
        lon === null
    ) {

        return;

    }


    // --------------------------------------------------------
    // Validate coordinate range
    // --------------------------------------------------------

    if (
        lat < -90 ||
        lat > 90 ||
        lon < -180 ||
        lon > 180
    ) {

        return;

    }


    // --------------------------------------------------------
    // Ignore zero GPS
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

        lat:
            lat,

        lon:
            lon,

        alt:
            alt !== null
                ? alt
                : null,

        timestamp:
            Date.now()

    };


    // --------------------------------------------------------
    // Store
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
    // Send to Team + Driver
    // --------------------------------------------------------

    io.emit(
        "routePoint",
        routePoint
    );

}


// ============================================================
// DASHBOARD ROUTES
// ============================================================

// ============================================================
// TEAM DASHBOARD
// ============================================================

app.get(
    "/",
    (req, res) => {

        res.sendFile(
            path.join(
                PUBLIC_DIR,
                "index.html"
            )
        );

    }
);


// ============================================================
// DRIVER DASHBOARD
// ============================================================

app.get(
    "/driver",
    (req, res) => {

        res.sendFile(
            path.join(
                PUBLIC_DIR,
                "driver.html"
            )
        );

    }
);


// ============================================================
// HEALTH CHECK
// ============================================================

app.get(
    "/health",
    (req, res) => {

        const mqttConnected =
            mqttClient
                ? mqttClient.connected
                : false;


        res.status(200).json({

            status:
                "ok",

            service:
                "Team Renew Telemetry Server",

            environment:
                process.env.NODE_ENV ||
                "development",

            port:
                PORT,

            publicUrl:
                PUBLIC_URL,

            mqttConnected:
                mqttConnected,

            mqttTopic:
                process.env.MQTT_TOPIC ||
                null,

            influxConfigured:
                Boolean(
                    process.env.INFLUX_URL &&
                    process.env.INFLUX_TOKEN &&
                    process.env.INFLUX_ORG &&
                    process.env.INFLUX_BUCKET
                ),

            routePoints:
                routeHistory.length,

            telemetryPackets:
                totalTelemetryPackets,

            invalidTelemetryPackets:
                invalidTelemetryPackets,

            telemetrySequence:
                telemetrySequence,

            lastTelemetryId:
                lastTelemetry
                    ? lastTelemetry.telemetry_id
                    : null,

            lastServerTimestamp:
                lastTelemetry
                    ? lastTelemetry.server_timestamp
                    : null,

            lastTelemetryReceived:
                lastTelemetryTime

        });

    }
);


// ============================================================
// SERVER START
// ============================================================
//
// IMPORTANT FOR RENDER:
//
// 0.0.0.0
// + process.env.PORT
//
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

        console.log("");

        console.log(
            `Environment: ${
                process.env.NODE_ENV ||
                "development"
            }`
        );

        console.log(
            `Port: ${PORT}`
        );

        console.log(
            "Host: 0.0.0.0"
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

        console.log("");

        console.log(
            "Telemetry synchronization:"
        );

        console.log(
            "✓ Same enriched packet → Team"
        );

        console.log(
            "✓ Same enriched packet → Driver"
        );

        console.log(
            "✓ Same telemetry_id → InfluxDB"
        );

        console.log(
            "✓ Same server_timestamp → InfluxDB"
        );

        console.log("");

        console.log(
            "================================================"
        );

        console.log("");

    }

);


// ============================================================
// MQTT EVENTS
// ============================================================

if (
    mqttClient
) {


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

                if (
                    error
                ) {

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

        console.error(
            "✗ MQTT Error:",
            error.message
        );

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

    async (
        topic,
        message
    ) => {

        try {

            // ==================================================
            // PARSE JSON
            // ==================================================

            const rawMessage =
                message.toString();


            let data;


            try {

                data =
                    JSON.parse(
                        rawMessage
                    );

            }

            catch (
                jsonError
            ) {

                invalidTelemetryPackets++;

                console.error(
                    "✗ Invalid MQTT JSON:"
                );

                console.error(
                    jsonError.message
                );

                console.error(
                    rawMessage
                );

                return;

            }


            // ==================================================
            // GENERATE SERVER TELEMETRY ID
            // ==================================================

            telemetrySequence++;


            const telemetryId =
                telemetrySequence;


            // ==================================================
            // SERVER TIMESTAMP
            // ==================================================

            const serverTimestamp =
                Date.now();


            // ==================================================
            // CREATE ONE ENRICHED PACKET
            // ==================================================
            //
            // IMPORTANT:
            //
            // We create this object ONCE.
            //
            // The exact same object is sent to:
            //
            // Team
            // Driver
            //
            // The same ID and timestamp are written to:
            //
            // InfluxDB
            //
            // ==================================================

            const telemetry = {

                ...data,

                telemetry_id:
                    telemetryId,

                server_timestamp:
                    serverTimestamp

            };


            // ==================================================
            // UPDATE SERVER STATE
            // ==================================================

            lastTelemetry =
                telemetry;

            lastTelemetryTime =
                serverTimestamp;

            totalTelemetryPackets++;


            // ==================================================
            // CONSOLE HEADER
            // ==================================================

            console.log("");

            console.log(
                "========== MQTT TELEMETRY =========="
            );

            console.log(
                `MQTT Topic: ${topic}`
            );

            console.log(
                `Telemetry ID: ${telemetryId}`
            );

            console.log(
                `Server Time: ${
                    new Date(
                        serverTimestamp
                    ).toISOString()
                }`
            );


            // ==================================================
            // PRINT IMPORTANT VALUES
            // ==================================================

            console.log(
                `Speed: ${
                    telemetry.speed_kmh
                } km/h`
            );

            console.log(
                `Throttle: ${
                    telemetry.throttle
               } %`
            );

            console.log(
                `Motor Power: ${
                    telemetry.m_power
                } W`
            );

            console.log(
                `Fuel Cell Power: ${
                    telemetry.fc?.power
                } W`
            );

            console.log(
                `Slope: ${
                    telemetry.slope
                } deg`
            );


            // ==================================================
            // SEND EXACT SAME PACKET
            // ==================================================
            //
            // BOTH dashboards receive this exact object.
            //
            // ==================================================

            io.emit(
                "telemetry",
                telemetry
            );


            console.log(
                `✓ Telemetry ${telemetryId} sent to Team`
            );

            console.log(
                `✓ Telemetry ${telemetryId} sent to Driver`
            );


            // ==================================================
            // INFLUXDB
            // ==================================================

            if (
                writeApi
            ) {

                const point =
                    new Point(
                        "telemetry"
                    );


                // =================================================
                // SYNCHRONIZATION FIELDS
                // =================================================

                point.intField(
                    "telemetry_id",
                    telemetryId
                );


                point.intField(
                    "server_timestamp",
                    serverTimestamp
                );


                // =================================================
                // VEHICLE TAG
                // =================================================

                if (
                    telemetry.vehicle !== undefined &&
                    telemetry.vehicle !== null
                ) {

                    point.tag(
                        "vehicle",
                        String(
                            telemetry.vehicle
                        )
                    );

                }


                // =================================================
                // VEHICLE DATA
                // =================================================

                addFloatField(
                    point,
                    "throttle",
                    telemetry.throttle
                );


                addFloatField(
                    point,
                    "accel",
                    telemetry.accel
                );


                addFloatField(
                    point,
                    "decel",
                    telemetry.decel
                );


                addFloatField(
                    point,
                    "slope",
                    telemetry.slope
                );


                addFloatField(
                    point,
                    "ds18b20_temp",
                    telemetry.ds18b20_temp
                );


                addFloatField(
                    point,
                    "bus_voltage",
                    telemetry.bus_voltage
                );


                addFloatField(
                    point,
                    "motor_current",
                    telemetry.motor_current
                );


                addFloatField(
                    point,
                    "m_rpm",
                    telemetry.m_rpm
                );


                addFloatField(
                    point,
                    "m_power",
                    telemetry.m_power
                );


                addFloatField(
                    point,
                    "speed_kmh",
                    telemetry.speed_kmh
                );


                addFloatField(
                    point,
                    "distance_m",
                    telemetry.distance_m
                );


                // =================================================
                // FUEL CELL
                // =================================================

                if (
                    telemetry.fc !== undefined &&
                    telemetry.fc !== null
                ) {

                    addFloatField(
                        point,
                        "fc_voltage",
                        telemetry.fc.voltage
                    );


                    addFloatField(
                        point,
                        "fc_current",
                        telemetry.fc.current
                    );


                    addFloatField(
                        point,
                        "fc_power",
                        telemetry.fc.power
                    );


                    addFloatField(
                        point,
                        "fc_stack_temp",
                        telemetry.fc.stack_temp
                    );


                    addFloatField(
                        point,
                        "fc_h2_leak_volts",
                        telemetry.fc.h2_leak_volts
                    );


                    addFloatField(
                        point,
                        "fc_env_temp",
                        telemetry.fc.env_temp
                    );


                    addFloatField(
                        point,
                        "fc_batt_voltage",
                        telemetry.fc.batt_voltage
                    );


                    addFloatField(
                        point,
                        "fc_batt_current",
                        telemetry.fc.batt_current
                    );


                    addIntegerField(
                        point,
                        "fc_error_flag",
                        telemetry.fc.error_flag
                    );

                }


                // =================================================
                // GPS
                // =================================================

                if (
                    telemetry.gps !== undefined &&
                    telemetry.gps !== null
                ) {

                    const lat =
                        numberOrNull(
                            telemetry.gps.lat
                        );


                    const lon =
                        numberOrNull(
                            telemetry.gps.lon
                        );


                    const alt =
                        numberOrNull(
                            telemetry.gps.alt
                        );


                    // ---------------------------------------------
                    // InfluxDB GPS
                    // ---------------------------------------------

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


                    // ---------------------------------------------
                    // Shared route
                    // ---------------------------------------------

                    addRoutePoint(
                        lat,
                        lon,
                        alt
                    );

                }


                // =================================================
                // CONNECTION STATUS
                // =================================================

                if (
                    telemetry.connected !== undefined
                ) {

                    point.intField(
                        "connected",
                        telemetry.connected
                            ? 1
                            : 0
                    );

                }


                // =================================================
                // WRITE
                // =================================================

                writeApi.writePoint(
                    point
                );


                console.log(
                    `✓ Telemetry ${telemetryId} queued for InfluxDB`
                );

            }

            else {

                console.error(
                    `✗ Telemetry ${telemetryId} NOT written: InfluxDB unavailable`
                );

            }


            // ==================================================
            // FINAL SYNC LOG
            // ==================================================

            console.log("");

            console.log(
                "SYNC VERIFICATION"
            );

            console.log(
                `Packet ID : ${telemetryId}`
            );

            console.log(
                `Team      : ${telemetryId}`
            );

            console.log(
                `Driver    : ${telemetryId}`
            );

            console.log(
                `InfluxDB  : ${telemetryId}`
            );

            console.log(
                "===================================="
            );

        }

        catch (
            error
        ) {

            console.error("");

            console.error(
                "✗ MQTT / TELEMETRY ERROR"
            );

            console.error(
                error.message
            );

            console.error("");

        }

    }

);

}


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
        // SEND EXISTING ROUTE
        // ====================================================

        socket.emit(
            "routeHistory",
            routeHistory
        );


        console.log(
            `✓ Sent ${
                routeHistory.length
            } GPS points to ${socket.id}`
        );


        // ====================================================
        // SEND LATEST TELEMETRY
        // ====================================================
        //
        // This is important.
        //
        // If a dashboard connects after telemetry has already
        // arrived, it immediately receives the latest packet.
        //
        // ====================================================

        if (
            lastTelemetry
        ) {

            socket.emit(
                "telemetry",
                lastTelemetry
            );


            console.log(
                `✓ Sent latest telemetry ${
                    lastTelemetry.telemetry_id
                } to ${socket.id}`
            );

        }


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
                    `✗ Dashboard disconnected: ${
                        socket.id
                    } (${reason})`
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


    shuttingDown =
        true;


    console.log("");

    console.log(
        "================================================"
    );

    console.log(
        "Shutting down telemetry server..."
    );

    console.log(
        "================================================"
    );


    // ========================================================
    // STOP HTTP SERVER
    // ========================================================

    try {

        await new Promise(
            (resolve) => {

                server.close(
                    () => {

                        console.log(
                            "✓ HTTP server closed"
                        );

                        resolve();

                    }
                );

            }
        );

    }

    catch (
        error
    ) {

        console.error(
            "HTTP shutdown error:",
            error.message
        );

    }


    // ========================================================
    // MQTT
    // ========================================================

    try {

        if (
            mqttClient
        ) {

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

    }

    catch (
        error
    ) {

        console.error(
            "MQTT shutdown error:",
            error.message
        );

    }


    // ========================================================
    // INFLUXDB
    // ========================================================

    try {

        if (
            writeApi
        ) {

            await writeApi.close();

            console.log(
                "✓ InfluxDB data flushed and connection closed"
            );

        }

    }

    catch (
        error
    ) {

        console.error(
            "InfluxDB shutdown error:",
            error.message
        );

    }


    console.log(
        "✓ Telemetry server shutdown complete"
    );


    process.exit(
        0
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


// ============================================================
// UNCAUGHT EXCEPTION
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


// ============================================================
// UNHANDLED REJECTION
// ============================================================

process.on(

    "unhandledRejection",

    (error) => {

        console.error(
            "✗ Unhandled Promise Rejection:",
            error
        );

    }

);