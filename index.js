const net = require('net');
const toPull = require('stream-to-pull-stream');
const pull = require('pull-stream');

const Pushable = require('pull-pushable');
const pullJson = require('pull-json-doubleline');
const pullDefer = require('pull-defer');

const zip  = require('pull-zip')

const uuidv4 = require('uuid/v4');

const debug = require('debug')('ssb-bluetooth-repeater');

const EventEmitter = require('events');
const delayedDeviceScanSource = pullDefer.source();

const localHost='127.0.0.1';


function makeManager (opts) {
  let scanActive = false;
  let rnBridge;

  const bluetoothScanStateEmitter = new EventEmitter();

  if (opts && opts.rnBridge) {
    rnBridge = opts.rnBridge;
  }
  else{
    rnBridge = require('rn-bridge')
  }

  if (!opts || !opts.myIdent) {
    throw new Error("ssb-mobile-bluetooth-manager must be configured with the myIdent option.")
  }

  if (!opts || !opts.metadataServiceUUID) {
    throw new Error("ssb-mobile-bluetooth-manager must be configured with a metadataServiceUUID option.");
  }

  if (!opts || !opts.controlPort) {
    throw new Error("ssb-mobile-bluetooth-manager must be configured with a controlSocketFilename option.");
  }

  if (!opts || !opts.incomingPort) {
    throw new Error("ssb-mobile-bluetooth-manager must be configured with a incomingSocketFilename option.");
  }

  if (!opts || !opts.outgoingPort) {
    throw new Error("ssb-mobile-bluetooth-manager must be configured with a outgoingSocketFilename option.");
  }

  /* Scanning while connected to another bluetooth device slows down the connection, increases latency and makes it
   * periodically disconnect, so we slow down the scan interval if we're gossiping with at least one other device.
   *
   * As per the android docs: https://developer.android.com/guide/topics/connectivity/bluetooth#QueryPairedDevices
   */
  const scanRefreshIntervalWhenConnected = opts.scanRefreshIntervalWhenConnected || 60000;
  let connectedDevices = 0;

  const EVENT_STARTED_SCAN = "startedBluetoothScan";
  const EVENT_FOUND_BLUETOOTH_DEVICES = "btDevicesFound";
  const EVENT_FINISHED_FINDING_BLUETOOTH_DEVICES = "endedBluetoothScan";
  const EVENT_CHECKING_DEVICES = "checkingForScuttlebutt";
  const EVENT_ENDED_CHECKING = "endedChecking";

  const awaitingConnection = Pushable();
  const outgoingConnectionsEstablished = Pushable();
  const outgoingAddressEstablished = Pushable();

  const incomingConnectionEstablished = Pushable();
  const incomingAddressEstablished = Pushable();

  let controlSocketSource = Pushable();

  let awaitingDevicesCb = null;
  let awaitingDiscoverableResponse = null;
  let awaitingIsEnabledResponse = null;
  let onIncomingConnection = null;
  let awaitingOwnMacAddressResponse = null;

  let awaitingMetadata = {

  }

  const metadataServiceUUID = opts.metadataServiceUUID;

  function connect(bluetoothAddress, cb) {
    debug("Attempting outgoing connection to bluetooth address: " + bluetoothAddress);

    awaitingConnection.push(cb);

    // Tell the native android code to make the outgoing bluetooth connection and then connect back
    // on the socket

    controlSocketSource.push({
      "command": "connect",
      "arguments": {
        "remoteAddress": bluetoothAddress
      }
    })

  }

  let controlSocketEstablished = false;

  function makeControlSocket() {
    if (controlSocketEstablished) return;

    let controlSocket = net.createServer(function(stream){

      let duplexConnection = toPull.duplex(stream);

      // Send commands to the control server
      pull(controlSocketSource,
        pullJson.stringify(),
        pull.map(logOutgoingCommand),
        duplexConnection.sink
      );

      // Receive and process commands from the control server
      pull(duplexConnection.source, pullJson.parse(), pull.drain(doCommand));

    }).listen({host:localHost,port:opts.controlPort});

    controlSocketEstablished = true;

    controlSocket.on('closed', function() {
      debug("Control socket closed");
      rnBridge.channel.post('log4RN', "Control socket closed");

    })
    controlSocket.on('error', function(err) {
      debug("Control socket error",err);
      rnBridge.channel.post('exception', err);
    })
    rnBridge.channel.post('log4RN', 'control socket create');

    debug("Created control socket");
  }

  function makeFullyEstablishConnectionsHandler() {

    // It's unpredictable when each of these things happen, but they do happen sequentially
    // within their stream, so we zip them together and do the necessary action when ready

    pull(
      zip(awaitingConnection, outgoingConnectionsEstablished, outgoingAddressEstablished),
      pull.drain( results => {

        let cb = results[0];
        let stream = results[1].stream;
        let connectionOutcome = results[2];

        let outgoingAddress = connectionOutcome.address;
        stream.address = outgoingAddress;

        if (connectionOutcome.success) {
          debug("Calling back multiserve with successful outgoing connection to " + outgoingAddress);
          rnBridge.channel.post('log4RN', "Calling back multiserve with successful outgoing connection to " + outgoingAddress);

          cb(null, stream);
        } else {
          debug("Calling back with unsuccessful connection to multiserver for address: " + outgoingAddress);
          rnBridge.channel.post('log4RN', "Calling back with unsuccessful connection to multiserver for address: " + outgoingAddress);

          cb(new Error(connectionOutcome.failureReason));
        }
      })
    );

    pull(
      zip(incomingConnectionEstablished, incomingAddressEstablished),
      pull.drain(results => {
        let stream = results[0].stream;
        let address = results[1].address;

        stream.address = address;
        rnBridge.channel.post('log4RN', 'incoming bluetooth connection:' + JSON.stringify(stream));

        debug("Calling back to multiserve with incoming bluetooth connection from " + address);
        onIncomingConnection(null, stream);
      })
    )

  }

  function logOutgoingCommand(command) {
    debug("Sending outgoing command to control server");
    debug(command);

    return command;
  }

  function doCommand (command) {

    debug("Received command: ");
    debug(command);

    rnBridge.channel.post('log4RN', 'Received command:' + JSON.stringify(command));

    let commandName = command.command;

    if (commandName === "connected" && !command.arguments.isIncoming) {
      // The initial stream connection is just to the Unix socket. We don't know if that socket is proxying
      // the bluetooth connection successfully until we receive an event to tell us it's connected.

      let addr = "bt:" + command.arguments.remoteAddress.split(":").join("");
      debug("Setting outgoing stream address to " + addr);

      let result = {
        success: true,
        address: addr
      }

      outgoingAddressEstablished.push(result);

      connectedDevices = connectedDevices + 1;
      debug("Connected bluetooth devices is now: " + connectedDevices);

    } else if (commandName === "connected" && command.arguments.isIncoming) {
      let incomingAddr = "bt:" + command.arguments.remoteAddress.split(":").join("");
      debug("Setting incoming connection stream address to: " + incomingAddr);

      incomingAddressEstablished.push({
        address: incomingAddr
      });

      connectedDevices = connectedDevices + 1;
      debug("Connected bluetooth devices is now: " + connectedDevices);

    } else if (commandName === "connectionFailure" && !command.arguments.isIncoming) {
      let reason = command.arguments.reason;

      let result = {
        success: false,
        address: command.arguments.remoteAddress.split(":").join(""),
        failureReason: reason
      }

      outgoingAddressEstablished.push(result);

    } else if (commandName === "disconnected") {
      connectedDevices = connectedDevices - 1;
      debug("Connected bluetooth devices is now: " + connectedDevices);

    } else if (commandName === "discovered") {
      let currentTime = Date.now();
      let args = command.arguments;

      debug("Updating nearby source");
      debug(args);

      if (args.error && args.errorCode === "bluetoothDisabled") {
        debug("Wanted nearby bluetooth devices but bluetooth is disabled. Will call back once bluetooth is enabled again.");
      }
      else if (args.error === true) {
        awaitingDevicesCb(new Error(args.description), null);
      } else {
        let nearBy = {
          lastUpdate: currentTime,
          discovered: args.devices
        }

        bluetoothScanStateEmitter.emit(EVENT_FOUND_BLUETOOTH_DEVICES, nearBy);
        bluetoothScanStateEmitter.emit(EVENT_FINISHED_FINDING_BLUETOOTH_DEVICES);

        awaitingDevicesCb(null, nearBy);
      }

    } else if (commandName === "discoverable") {
      let args = command.arguments;
      if (args.error === true) {
        awaitingDiscoverableResponse(command.arguments);
      }
      else {
        awaitingDiscoverableResponse(null, command.arguments);
      }
    } else if (commandName === "isEnabled") {
      let args = command.arguments;
      awaitingIsEnabledResponse(null, args.enabled);
    } else if (commandName === "ownMacAddress") {
      let args = command.arguments;
      awaitingOwnMacAddressResponse(null, args.address);
    } else if (commandName === "getMetadata") {
      let args = command.arguments;

      let requestId = command.requestId;

      let cb = awaitingMetadata[requestId];

      if (args.error === true) {
        cb(new Error(args.error.description), null);
      } else {
        cb(null, args.metadata);
      }

      delete awaitingMetadata[requestId];

    } else if (commandName === "bluetoothState" && command.arguments.isEnabled) {

      debug("Bluetooth has been enabled.");

      if (awaitingDevicesCb) {
        debug("Was awaiting nearby devices callback but bluetooth was previously disabled. Making request again now.");
        getLatestNearbyDevices(awaitingDevicesCb);
      }

    }

  }

  function listenForOutgoingEstablished() {

    let server = net.createServer(function(stream){
      debug("bluetooth: Outgoing connection established proxy connection.")

      let item = {
        stream: logDuplexStreams(toPull.duplex(stream), 'outgoing')
      }

      outgoingConnectionsEstablished.push(item);

    });

    server.on('listening', () => {
      debug("Server listening for outgoing connections. Starting control unix socket.");
      makeControlSocket();
    });
    server.on('close', function (e) {
      rnBridge.channel.post('log4RN', "outgoing socket closed: " + e);

      debug("bt_bridge socket closed: " + e);
    });
    server.on('error', function(err) {
      debug("outgoing socket error",err);
      rnBridge.channel.post('exception', err);
    });

    rnBridge.channel.post('log4RN', 'outgoing socket create');

    return server.listen({host:localHost,port:opts.outgoingPort});
  }

  // For some reason, .server gets called twice...
  let started = false

  function listenForIncomingConnections(onConnection) {

    onIncomingConnection = onConnection;

    if(started) return
    
    let server = net.createServer(function (incomingStream) {

      // We only call back with the connection when we later receive the address over the control
      // bridge. See the 'onCommand' function.

      let item = {
        stream: logDuplexStreams(toPull.duplex(incomingStream), 'incoming')
      }
      rnBridge.channel.post('log4RN', 'incoming socket stream:' + JSON.stringify(item.stream));

      incomingConnectionEstablished.push(item)

    }).listen({host:localHost,port:opts.incomingPort});
    server.on('error', function(err) {
      debug("incoming socket error",err);
      rnBridge.channel.post('exception', err);
    })
    server.on('close', function (e) {
      rnBridge.channel.post('log4RN', "incoming socket closed: " + e);

      debug("bt_bridge socket closed: " + e);
    });
    rnBridge.channel.post('log4RN', 'incoming socket create');

    started = true;

    return function () {
      debug("Server close?");
    }
  }

  function refreshNearbyDevices() {
    // Tell the native android code to discover nearby devices. When it responds, we'll update the
    // 'nearBy devices' pull-stream source

    controlSocketSource.push({
      "command": "discoverDevices",
      "arguments": {

      }
    });
  }

  function getLatestNearbyDevices(cb) {
    awaitingDevicesCb = cb;

    refreshNearbyDevices();
  }

  function getValidAddresses(devices, cb) {

    let results = [];
    let count = 0;

    if (devices.length === 0) {
      debug("No nearby devices to check for scuttlebutt metadata service.");
      cb(null, {
        "discovered": results,
        "lastUpdate": Date.now()
      });

      return;
    }

    devices.forEach( (device, num) => {

      getMetadataForDevice(device.remoteAddress, (err, res) => {

        count = count + 1;
        debug("getValidAddresses count: " + count);

        if (!err) {
          debug(device.remoteAddress + " is available for scuttlebutt bluetooth connections");
          device.id = res.id;
          results.push(device);
        }

        if (count === devices.length) {
          debug("Calling back (get valid addresses)...");
          debug("Valid addresses:");
          debug(results);

          bluetoothScanStateEmitter.emit(EVENT_CHECKING_DEVICES, {
            "checked": count,
            "total": devices.length,
            "discovered": results,
            "found": results.length,
            "remaining": (devices.length - count),
            "lastUpdate": Date.now()
          });

          cb(null, {
            "discovered": results,
            "lastUpdate": Date.now()
          });
        } else {
          bluetoothScanStateEmitter.emit(EVENT_CHECKING_DEVICES, {
            "checked": count,
            "total": devices.length,
            "discovered": results,
            "found": results.length,
            "remaining": (devices.length - count),
            "lastUpdate": Date.now()
          });
        }

      });
    })
  }

  function nearbyScuttlebuttDevices(refreshInterval) {

    return pull(
      nearbyDevices(refreshInterval),
      pull.asyncMap( (result, cb) => {

        debug("Nearby bluetooth devices.");
        debug(result);

        getValidAddresses(result.discovered, cb)
      }),
      pull.map(result => {
        bluetoothScanStateEmitter.emit(EVENT_ENDED_CHECKING, result);
        return result;
      })
    )
  }

  function nearbyDevices(refreshInterval) {

    return pull(
      // We don't start scanning until the user has made their own device discoverable, signalling they wish
      // to use the bluetooth functionality.
      delayedDeviceScanSource,
      pull.asyncMap((next, cb) => {

        let nextScanAfter = refreshInterval;

        if (connectedDevices > 0) {
          debug("Connected device count is " + connectedDevices + ". Next scan will start after " + scanRefreshIntervalWhenConnected + " milliseconds");
          nextScanAfter = scanRefreshIntervalWhenConnected;
        } else {
          debug("Starting next scan after: " + nextScanAfter);
        }

        setTimeout(() => {
          bluetoothScanStateEmitter.emit(EVENT_STARTED_SCAN);
          getLatestNearbyDevices(cb)
        }, nextScanAfter)
      })
    )
  }

  function makeDeviceDiscoverable(forTime, cb) {
    debug("Making device discoverable");
    rnBridge.channel.post('log4RN', 'Making device discoverable');
    setTimeout(() => {
      awaitingDiscoverableResponse = null;
    }, forTime);
    if (awaitingDiscoverableResponse != null) {
      cb(new Error("Already requesting to make device discoverable."), null)
    } else {
      awaitingDiscoverableResponse = (err, result) => {

        if (!scanActive) {
          delayedDeviceScanSource.resolve(pull.infinite());
          scanActive = true;
        }

        if (err) {
          cb(new Error(err.description), null);
        } else {

          let payload = {
            "id": opts.myIdent
          };

          // The service should stop when the device is no longer discoverable
          let serviceNeededForSeconds = Math.ceil((result.discoverableUntil - Date.now()) / 1000);

          // Only start the metadata service once the device is discoverable
          controlSocketSource.push({
            "command": "startMetadataService",
            "arguments": {
              "serviceName": "scuttlebuttMetadata",
              "service": metadataServiceUUID,
              "payload": payload,
              "timeSeconds": serviceNeededForSeconds
            }
          });

          cb(null, result);
        }

      };

      controlSocketSource.push({
        "command": "makeDiscoverable",
        "arguments": {
          "forTime": forTime
        }
      });
    }
  }

  function isEnabled(cb) {
    if (awaitingIsEnabledResponse) {
      cb(new Error("Already awaiting 'isEnabled' response."), null);
    } else {
      awaitingIsEnabledResponse = cb;

      controlSocketSource.push({
        "command": "isEnabled",
        "arguments": {

        }
      })
    }
  }

  function getMetadataForDevice(deviceMacAddress, cb) {
    let requestId = uuidv4();

    awaitingMetadata[requestId] = cb;

    controlSocketSource.push({
      "command": "getMetadata",
      "requestId": requestId,
      "arguments": {
        "remoteDevice": deviceMacAddress,
        "service": metadataServiceUUID
      }
    });

  }

  function bluetoothScanState() {

    let source = Pushable(function (closed) {

      debug("Closing bluetooth scan lifecycle event listeners.");

      bluetoothScanStateEmitter.removeListener(EVENT_STARTED_SCAN, onScanStarted);
      bluetoothScanStateEmitter.removeListener(EVENT_FOUND_BLUETOOTH_DEVICES, onBtDevicesFound);
      bluetoothScanStateEmitter.removeListener(EVENT_FINISHED_FINDING_BLUETOOTH_DEVICES, onFinishedFindingBluetoothDevices);
      bluetoothScanStateEmitter.removeListener(EVENT_CHECKING_DEVICES, onCheckingDevices);
      bluetoothScanStateEmitter.removeListener(EVENT_ENDED_CHECKING, onFinishedCheckingDevices);
    });

    function onScanStarted()  {
      let event = {
        "state": EVENT_STARTED_SCAN
      }

      source.push(event);
    };

    function onBtDevicesFound (devices) {
      let event = {
        "state": EVENT_FOUND_BLUETOOTH_DEVICES,
        "update": devices
      }

      source.push(event);
    }

    function onFinishedFindingBluetoothDevices() {
      let event = {
        "state": EVENT_FINISHED_FINDING_BLUETOOTH_DEVICES
      }

      source.push(event);
    }

    function onCheckingDevices (update) {
      let event = {
        "state": EVENT_CHECKING_DEVICES,
        "update": update
      }

      source.push(event);
    }

    function onFinishedCheckingDevices() {
      let event = {
        "state": EVENT_ENDED_CHECKING
      }

      source.push(event);
    }

    bluetoothScanStateEmitter.on(EVENT_STARTED_SCAN, onScanStarted);
    bluetoothScanStateEmitter.on(EVENT_FOUND_BLUETOOTH_DEVICES, onBtDevicesFound);
    bluetoothScanStateEmitter.on(EVENT_FINISHED_FINDING_BLUETOOTH_DEVICES, onFinishedFindingBluetoothDevices);
    bluetoothScanStateEmitter.on(EVENT_CHECKING_DEVICES, onCheckingDevices);
    bluetoothScanStateEmitter.on(EVENT_ENDED_CHECKING, onFinishedCheckingDevices);

    return source;
  }

  function getOwnMacAddress(cb) {
    if (awaitingOwnMacAddressResponse) {
      return makeError("alreadyAwaitingMacAddress", "Already awaiting 'ownMacAddress' response");
    } else {
      awaitingOwnMacAddressResponse = cb;

      controlSocketSource.push({
        "command": "ownMacAddress",
        "arguments": {

        }
      })

    }
  }

  function makeError(errorCode, description) {
    return {
        "error": true,
        "errorCode": errorCode,
        "description": description
      }
  }

  /**
   * If 'opts.logStreams' is true, logs out incoming and outgoing data streams.
   * @param {} duplexStream
   */
  function logDuplexStreams(duplexStream, tag) {
    // if (!opts.logStreams) {
    //   return duplexStream;
    // } else {

      duplexStream.source = pull(duplexStream.source, pull.map(
        buff => {
          debug( "[source] " + buff.toString() )
          rnBridge.channel.post('log4RN',  tag + "[source] " + buff.toString() );
          return buff;
        }
      ));

      duplexStream.sink = pull(
        pull.map(outgoingBuff => {
          debug( "[sink] " + outgoingBuff.toString() )
          rnBridge.channel.post('log4RN', tag + "[sink] " + outgoingBuff.toString());
          return outgoingBuff;
        }),
        duplexStream.sink
      )

      return duplexStream;
    // }
  }


  listenForOutgoingEstablished();
  makeFullyEstablishConnectionsHandler();
  rnBridge.channel.post('log4RN', 'bluetooth manager init');

  return {
    connect,
    listenForIncomingConnections,
    nearbyDevices,
    bluetoothScanState,
    nearbyScuttlebuttDevices,
    makeDeviceDiscoverable,
    getMetadataForDevice,
    isEnabled,
    getOwnMacAddress
  }

}

module.exports = makeManager;
