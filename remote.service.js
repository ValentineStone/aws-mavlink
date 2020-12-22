const AWS = require('aws-sdk')
const AWSMqttClient = require('aws-mqtt/lib/NodeClient')
const SerialPort = require('serialport')
const path = require('path')
const { debounce } = require('lodash')
const { MAVLink20Processor, mavlink20 } = require('./MAVLink20')

// Load configs

const config = require('./config.json')
const { resolve } = require('path')

AWS.config.loadFromPath(path.join(__dirname, 'aws.keys.json'))

const error_cb = (resolve, reject, value = undefined) =>
  (error, value_arg) => error
    ? reject(error)
    : resolve(value_arg || value)

// Create resources

const mqttclient = new AWSMqttClient({
  region: AWS.config.region,
  credentials: AWS.config.credentials,
  endpoint: config.endpoint,
  reconnectPeriod: 0,
  connectTimeout: config.restartDelay
})

const serialport = new SerialPort(
  config.serial.path,
  {
    baudRate: config.serial.baudRate,
    autoOpen: false
  },
  exitOnError
)

const mav2 = new MAVLink20Processor()

const run = async () => {

  // Connect all together

  const mqtt_connected_and_subscribed = new Promise((resolve, reject) =>
    mqttclient.on('connect', () => {
      mqttclient.subscribe(config.topicToThing, error_cb(resolve, reject))
    })
  )

  const serial_opened = new Promise((resolve, reject) =>
    serialport.on('open', error_cb(resolve, reject))
  )

  serialport.open()
  mqttclient.reconnect()

  await serial_opened
  await mqtt_connected_and_subscribed

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

  await new Promise((resolve, reject) => {
    serialport.on('error', reject)
    serialport.on('close', () => reject(new Error('Serialport closed')))
    mqttclient.on('error', reject)
    mqttclient.on('close', () => reject(new Error('MQTTClient closed')))
    mqttclient.on('disconnect', () => reject(new Error('MQTTClient disconnected')))
    mqttclient.on('offline', () => reject(new Error('MQTTClient went offline')))
  })

}

const stop = () => new Promise(r => mqttclient.end(true, () => serialport.close(r)))


// Utils

const pong = debounce(() => {
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

const wait = ms => new Promise(r => setTimeout(r, ms))

const rerun = () => {
  run().catch(error => {
    console.error(error.message)
    return stop()
  }).then(() => {
    console.log('Restarting...')
    return wait(config.restartDelay).then(rerun)
  })
}

rerun('initial')