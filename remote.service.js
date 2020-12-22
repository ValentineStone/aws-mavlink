const AWS = require('aws-sdk')
const AWSMqttClient = require('aws-mqtt/lib/NodeClient')
const SerialPort = require('serialport')
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
  reconnectPeriod: config.restartDelay,
  connectTimeout: config.restartDelay,
})
mqttclient.on('connect', () => {
  mqttclient.subscribe(config.topicToThing)
  console.log('MQTT connected')
})
mqttclient.on('error', () => mqttclient.reconnect())

const mav2 = new MAVLink20Processor()


// Connect all together

const run = async () => {

  let serialport

  try {
    console.log('Connecting...')

    serialport = new SerialPort(
      config.serial.path, {
      baudRate: config.serial.baudRate,
      autoOpen: false
    })

    await new Promise((resolve, reject) => {
      serialport.on('error', reject)
      serialport.on('open', error => error ? reject(error) : resolve())
      serialport.open()
    })

    console.log('Serialport connected')

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

    mqttclient.removeAllListeners('message')
    mqttclient.on('message', (topic, buff) => {
      if (serialport.isOpen) {
        console.log('recv', buff.length)
        serialport.write(buff)
      }
      else
        console.log('skip', 'recv', buff.length)
    })

    await new Promise((resolve, reject) => {
      serialport.on('error', reject)
      serialport.on('close', () => reject(new Error('Serialport closed')))
    })
  }
  catch (error) {
    console.log('Stopping...')
    await new Promise(r => serialport.close(r))
    console.log('Stopped Serialport')
    return error
  }
}


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
  run().then(error => {
    console.log(error.message)
    console.log('Waiting restart timeout...')
    wait(config.restartDelay)
      .then(() => console.log('Restarting...'))
      .then(rerun)
  })
}

rerun()