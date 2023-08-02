"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || function (mod) {
    if (mod && mod.__esModule) return mod;
    var result = {};
    if (mod != null) for (var k in mod) if (k !== "default" && Object.prototype.hasOwnProperty.call(mod, k)) __createBinding(result, mod, k);
    __setModuleDefault(result, mod);
    return result;
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.Kafka = void 0;
/**
 * This module provides a Kafka integration connecting to a Kafka server.
 */
const node_rdkafka_1 = require("node-rdkafka");
const fs_1 = require("fs");
const path = __importStar(require("path"));
class Kafka {
    /**
     * Create a new Kafka instance with the given group ID.
     * @param groupId - The group ID for the Kafka consumer.
     */
    constructor(groupId) {
        /**
         * Kafka producer instance.
         */
        this.producer = null;
        /**
         * The group ID for the Kafka consumer.
         */
        this.groupId = null;
        /**
         * Kafka consumer instance.
         */
        this.consumer = null;
        /**
         * Configuration options for Kafka producer and consumer.
         */
        this.config = {};
        /**
         * The file name of the Kafka client properties file.
         */
        this.fileName = path.join(__dirname, '../client.properties');
        this.groupId = groupId;
        this.setup();
    }
    /**
     * Setup the Kafka producer and consumer by reading configuration and initializing connections.
     * @private
     */
    async setup() {
        this.config = this.readConfigFile(this.fileName);
        this.config["group.id"] = this.groupId;
        await this.setupProducer();
        this.setupConsumer();
    }
    /**
     * Read the Kafka client properties file and return the configuration options as an object.
     * @param fileName - The file name of the Kafka client properties file.
     * @returns The configuration options as an object.
     * @private
     */
    readConfigFile(fileName) {
        const data = (0, fs_1.readFileSync)(fileName, 'utf8').toString().split('\n');
        return data.reduce((config, line) => {
            const [key, value] = line.split('=');
            if (key && value) {
                config[key.trim()] = value.trim();
            }
            return config;
        }, {});
    }
    /**
     * Setup the Kafka producer and connect to the Kafka broker.
     * @private
     */
    async setupProducer() {
        var _a;
        console.log('setup Kafka Producer...');
        this.producer = new node_rdkafka_1.Producer(this.config);
        this.handleErrors();
        await ((_a = this.producer) === null || _a === void 0 ? void 0 : _a.connect());
        console.log('Kafka Producer is ready');
    }
    /**
     * Setup the Kafka consumer and connect to the Kafka broker.
     * @private
     */
    setupConsumer() {
        console.log('setup Kafka Consumer...');
        this.consumer = new node_rdkafka_1.KafkaConsumer(this.config, { "auto.offset.reset": "earliest" });
        console.log('Kafka Consumer is ready');
    }
    /**
     * Produce a message to the specified Kafka topic with an optional scheduling time.
     * @param topic - The Kafka topic to produce the message to.
     * @param message - The message to be sent to Kafka.
     * @param date - Optional scheduling time for the message. If provided, the message will be scheduled for the given time.
     * @returns A Promise that resolves when the message is sent to Kafka.
     */
    async produce(topic, message, date) {
        var _a;
        const msg = JSON.stringify(message);
        const scheduleMsg = date ? new Date(date).getTime() - Date.now() : 0;
        (_a = this.producer) === null || _a === void 0 ? void 0 : _a.on('ready', () => {
            var _a;
            console.log(`Kafka Producer sending the message: ${message} to topic: ${topic} and schedule it to: ${date}`);
            (_a = this.producer) === null || _a === void 0 ? void 0 : _a.produce(topic, null, Buffer.from(msg), null, scheduleMsg);
        });
    }
    /**
     * Activate the Kafka consumer for the specified topic and handle message consumption.
     * @param topic - The Kafka topic to consume messages from.
     * @param consumerCallback - The callback function to handle the consumed Kafka message.
     * @returns A Promise that resolves when the consumer is activated.
     */
    async activateConsumer(topic, consumerCallback) {
        var _a, _b;
        (_a = this.consumer) === null || _a === void 0 ? void 0 : _a.connect();
        (_b = this.consumer) === null || _b === void 0 ? void 0 : _b.on('ready', () => {
            var _a, _b;
            (_a = this.consumer) === null || _a === void 0 ? void 0 : _a.subscribe([topic]);
            (_b = this.consumer) === null || _b === void 0 ? void 0 : _b.consume();
        }).on("data", (message) => {
            var _a;
            try {
                consumerCallback(null, message);
                (_a = this.consumer) === null || _a === void 0 ? void 0 : _a.commitMessage(message);
                console.log(`a message is commited: ${message}`);
            }
            catch (err) {
                consumerCallback(err, null);
                console.error('Error sending notification: ', err);
            }
        });
    }
    /**
     * Close the Kafka producer and consumer connections.
     * @returns A Promise that resolves when the connections are closed.
     */
    async close() {
        var _a, _b;
        try {
            (_a = this.producer) === null || _a === void 0 ? void 0 : _a.disconnect();
            (_b = this.consumer) === null || _b === void 0 ? void 0 : _b.disconnect();
            console.log('Disconnected from Kafka. Esiting...');
        }
        catch (err) {
            console.error('Error closing connections: ', err);
        }
    }
    /**
     * Setup error handling for the Kafka producer and consumer.
     * @private
     */
    handleErrors() {
        var _a, _b;
        (_a = this.producer) === null || _a === void 0 ? void 0 : _a.on('event.error', (err) => {
            console.error('Error from Kafka Producer: ', err);
        });
        (_b = this.consumer) === null || _b === void 0 ? void 0 : _b.on('event.error', (err) => {
            console.error('Error from Kafka Consumer: ', err);
        });
    }
}
exports.Kafka = Kafka;
