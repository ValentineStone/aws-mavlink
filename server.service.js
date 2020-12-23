const AWS = require('aws-sdk')
const AWSMqttClient = require('aws-mqtt/lib/NodeClient')
const dgram = require('dgram')
const path = require('path')
const { debounce } = require('lodash')
const { MAVLink20Processor, mavlink20 } = require('./MAVLink20')

// Load configs

const config = require('./config.json')

AWS.config.loadFromPath(path.join(__dirname, 'aws.keys.json'))

// Create resources

const mqttclient = new AWSMqttClient({
  region: AWS.config.region,
  credentials: AWS.config.credentials,
  endpoint: config.endpoint,
  reconnectPeriod: 1000,
})
mqttclient.on('connect', () => {
  mqttclient.subscribe(config.topicFromThing)
  console.log('MQTT connected')
})
mqttclient.on('error', () => mqttclient.reconnect())

const mav2 = new MAVLink20Processor()

const udp_socket = dgram.createSocket('udp4')
udp_socket.bind(config.udp.server || {})

// Connect all together

mqttclient.on('message', (topic, buff) => {
  console.log('recv', buff.length)
  udp_socket.send(buff, config.udp.gcs.port, config.udp.gcs.address)
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

udp_socket.on('listening', () => {
  const address = udp_socket.address()
  console.log(`udp_socket listening ${address.address}:${address.port}`)
})