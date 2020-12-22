#!/usr/bin/env node
const AWS = require('aws-sdk')
const AWSMqttmqttclient = require('aws-mqtt/lib/NodeClient')
const dgram = require('dgram')
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

const udp_socket = dgram.createSocket('udp4')
let udp_socket_isOpen = false
udp_socket.bind(config.udp.server || {}, () => udp_socket_isOpen = true)

const mav2 = new MAVLink20Processor()

// Connect all together

mqttclient.on('connect', () => {
  mqttclient.subscribe(config.topicFromThing, exitOnError)
})

mqttclient.on('message', (topic, buff) => {
  if (udp_socket_isOpen) {
    console.log('recv', buff.length)
    udp_socket.send(buff, config.udp.gcs.port, config.udp.gcs.address)
  }
  else
    console.log('skip', 'recv', buff.length)
})

udp_socket.on('message', buff => {
  for (const message of mav2.parseBuffer(buff)) {
    if (message instanceof mavlink20.messages.bad_data) {
      console.log('skip', 'send', message.msgbuf.length, 'as', message.name)
    }
    else {
      if (mqttclient.connected) {
        console.log('send', message.msgbuf.length, 'as', message.name)
        mqttclient.publish(config.topicToThing, message.msgbuf)
      }
      else {
        console.log('skip', 'send', message.msgbuf.length, 'as', message.name)
      }
    }
  }
})

udp_socket.on('error', exitOnError)
udp_socket.on('close', () => {
  udp_socket_isOpen = false
  exitOnError(new Error('UPD socket closed'))
})
mqttclient.on('error', exitOnError)
mqttclient.on('close', () => exitOnError(new Error('MQTTClient closed')))
mqttclient.on('disconnect', () => exitOnError(new Error('MQTTClient disconnected')))
mqttclient.on('offline', () => exitOnError(new Error('MQTTClient went offline')))

// Cleanup on exit

process.on('exit', () => {
  mqttclient.end(true)
  udp_socket.close()
})

// Utils

function exitOnError(error) {
  if (error) {
    console.error(error.message)
    process.exit(1)
  }
}