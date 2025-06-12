const { Kafka } = require('kafkajs');
const moment = require('moment-timezone');
require('dotenv').config();

// Kafka configuration
const kafka = new Kafka({
    clientId: 'sensor-data-producer',
    brokers: ["my-cluster-kafka-bootstrap.kafka:9092"],
    sasl: {
        mechanism: "scram-sha-512",
        username: process.env.KAFKA_SASL_USERNAME,
        password: process.env.KAFKA_SASL_PASSWORD,
    },
});

const producer = kafka.producer();
const topic = process.env.KAFKA_TOPIC;

async function publishMessage() {
    try {
        await producer.connect();
        console.log("Connected to Kafka broker");

        setInterval(async () => {
            const message = generateSensorData();
            
          await producer.send({
                topic: topic,
                messages: [{
                    key: message.device_id, // or any logic to generate a key
                    value: JSON.stringify(message)
                }],
            });


            console.log("Published sensor data to Kafka:", message);
        }, 5000);
    } catch (error) {
        console.error("Error publishing message:", error);
    }
}
function generateSensorData() {
    const batteryLevel = Math.floor(Math.random() * 50); // Random number between 0 and 49

    return {
        device_id: "SM-R920",
        timestamp: moment().utc().format(),
        user_id: "123456789",
        name: "Sridhar",
        age: 27,
        gender: "Male",
        height_cm: 175,
        weight_kg: 70.5,
        fitness_level: "Intermediate",
        daily_steps_goal: 10000,
        daily_calories_goal: 2500,
        sleep_hours_goal: 7.5,
        heart_rate_bpm: Math.floor(Math.random() * (100 - 60) + 60),
        accelerometer_x: (Math.random() * 2 - 1).toFixed(2),
        accelerometer_y: (Math.random() * 2 - 1).toFixed(2),
        accelerometer_z: (Math.random() * 2 - 1).toFixed(2),
        gyroscope_x: (Math.random() * 2 - 1).toFixed(3),
        gyroscope_y: (Math.random() * 2 - 1).toFixed(3),
        gyroscope_z: (Math.random() * 2 - 1).toFixed(3),
        temperature_C: (35 + Math.random() * 5).toFixed(1),
        blood_oxygen_percent: Math.floor(Math.random() * (100 - 90) + 90),
        step_count: Math.floor(Math.random() * 10000),
        calories_burned_kcal: Math.floor(Math.random() * 500),
        battery_level_percent: Math.floor(Math.random() * 100),
        charging_status: batteryLevel < 30, // ✅ True if battery < 30, else False
        bluetooth: Math.random() > 0.5, // ✅ Random True or False
        wifi: Math.random() > 0.5, // ✅ Random True or False
        lte: Math.random() > 0.5, // ✅ Random True or False
        status: Math.random() > 0.5 // ✅ Random True or False
    };
}

// Start publishing sensor data to Kafka
publishMessage();
