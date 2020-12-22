#!/usr/bin/env node
const AWS = require('aws-sdk')
const AWSMqttmqttclient = require('aws-mqtt/lib/NodeClient')
const SerialPort = require('serialport')
const path = require('path')
const { throttle } = require('lodash')
const { MAVLink20Processor, mavlink20 } = require('./MAVLink20')

// Load configs

const config = require('./config.json')

AWS.config.loadFromPath(path.join(__dirname, 'aws.keys.json'))

// Create resources

const mqttclient = new AWSMqttmqttclient({
  region: AWS.config.region,
  credentials: AWS.config.credentials,
  endpoint: config.endpoint,
  reconnectPeriod: 0,
  will: {
    topic: '/will',
    payload: 'Connection Closed abnormally..!',
    qos: 0,
    retain: false
  }
})

const serialport = new SerialPort(
  config.serial.path,
  { baudRate: config.serial.baudRate },
  exitOnError
)

const mav2 = new MAVLink20Processor()

// Connect all together

mqttclient.on('connect', () => {
  mqttclient.subscribe(config.topicToThing, exitOnError)
})

mqttclient.on('message', (topic, buff) => {
  if (serialport.isOpen) {
    console.log('recv', buff.length)
    serialport.write(buff)
  }
  else
    console.log('skip', 'recv', buff.length)
})

serialport.on('data', buff => {
  for (const message of mav2.parseBuffer(buff)) {
    if (message instanceof mavlink20.messages.bad_data) {
      pong()
    }
    else {
      if (mqttclient.connected) {
        console.log('send', message.msgbuf.length, 'as', message.name)
        mqttclient.publish(config.topicFromThing, message.msgbuf)
      }
      else {
        console.log('skip', 'send', message.msgbuf.length, 'as', message.name)
      }
    }
  }
})

serialport.on('error', exitOnError)
serialport.on('close', () => exitOnError(new Error('Serialport closed')))
mqttclient.on('close', () => exitOnError(new Error('MQTTClient closed')))
mqttclient.on('disconnect', () => exitOnError(new Error('MQTTClient disconnected')))
mqttclient.on('offline', () => exitOnError(new Error('MQTTClient went offline')))

// Cleanup on exit


process.on('exit', () => {
  mqttclient.end(true)
  serialport.close()
})

// Utils

function exitOnError(error) {
  if (error) {
    console.error(error.message)
    process.exit(1)
  }
}

const pong = throttle(() => {
  console.log('pong')
  serialport.write(
    Uint8Array.from(
      mav2.send(
        new mavlink20.messages.command_long(
          config.sysid, 1, 0,
          mav2.MAV_CMD_REQUEST_MESSAGE,
          mav2.MAVLINK_MSG_ID_PROTOCOL_VERSION
        )
      )
    )
  )
}, config.pongThrottle)