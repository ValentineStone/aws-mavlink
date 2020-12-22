const AWS = require('aws-sdk')
const AWSMqttmqttclient = require('aws-mqtt/lib/NodeClient')
const SerialPort = require('serialport')
const path = require('path')
const { throttle } = require('lodash')
const { MAVLink20Processor, mavlink20 } = require('./MAVLink20')

// Load configs

const config = require('./config.json')

AWS.config.loadFromPath(path.join(__dirname, 'aws.keys.json'))

const run = () => new Promise((resolve, reject) => {

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
    {
      baudRate: config.serial.baudRate,
      //autoOpen: false
    },
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
  mqttclient.on('error', exitOnError)
  mqttclient.on('close', () => exitOnError(new Error('MQTTClient closed')))
  mqttclient.on('disconnect', () => exitOnError(new Error('MQTTClient disconnected')))
  mqttclient.on('offline', () => exitOnError(new Error('MQTTClient went offline')))

  // Cleanup on exit

  function exitOnError(error) {
    if (error) {

      mqttclient.end(true, () => {
        console.error('MQTTCLIENT CLOSED')
        if (serialport.isOpen) {
          serialport.close(err => {
            console.error('SERIALPORT CLOSED', err)
            reject(error)
          })
        }
        else {
          console.error('SERIALPORT NOT OPEN')
          reject(error)
        }
      })
    }
  }

  // Utils

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

})

const wait = ms => new Promise(r => setTimeout(r, ms))

const rerun = () => {
  run().catch(error => {
    console.error(error.message)
    console.log('Restarting...')
    wait(config.restartDelay).then(rerun)
  })
}

rerun()