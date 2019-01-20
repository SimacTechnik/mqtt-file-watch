const fs = require('fs');
const mqtt = require('mqtt');
const glob = require("glob");

const requiredParams = 6;

const help = "Usage: node app.js [FILEPATH] [MQTT_ADDRESS] [MQTT_USERNAME] [MQTT_TOPIC] [OPTIONS]...\n\n"+
            "OPTIONS:\n"+
            "-m <regex>, --migrate=<regex>   Load and send all files matching regex first (sorted by modified date)\n"+
            "-b --begin                      Read listened file from beggining first\n"
            "--help, -h                      Print help";

if(process.argv.length < requiredParams)
    err("You need to specify all required values\n\n"+help);

const filePath = process.argv[2];

const mqttUrl = process.argv[3];

const mqttUsername = process.argv[4];

const mqttTopic = process.argv[5];

const client = mqtt.connect(mqttUrl, {username: mqttUsername, clean: false});

const maxObjCount = 10;

let migrationRegex = null;

let begin = false;

let connected = false;

let dataBuffer = [];

let sendInterval = null;

let debugging = false;

let sending = false;

if(process.argv.length > requiredParams) {
    for(let i = requiredParams; i < process.argv.length; i++) {
        switch(process.argv[i]) {
            case "-h":
            case "--help":
                process.stdout.write(help);
                process.exit(0);
            case "-m":
                if(process.argv.length == i+1)
                    err("Please specify regex for option -m");
                migrationRegex = process.argv[++i];
                break;
            case "-b":
            case "--begin":
                begin = true;
                break;
            case "-d":
            case "--debug":
                debugging = true;
                break;
            default:
                if(!process.argv[i].startsWith("--migrate="))
                    err("Unknown option\n\n"+help);
                migrationRegex = process.argv[i].substr(10);
        }
    }
}

client.on('connect', function () {
    debug("in client.on 'connect' function");
    log("Connected to MQTT");
    connected = true;
    log("Starting interval for buffer handling");
    sendInterval = setInterval(handleBuffer, 200);
 });

 client.on('reconnect', function () {
    debug("in client.on 'reconnect' function");
    //log("Reconnecting to MQTT");
     connected = false;
     if(sendInterval != null) {
        clearInterval(sendInterval);
        sendInterval = null;
     }
 });

 client.on('close', function () {
    debug("in client.on 'close' function");
    //log("MQTT connection closed");
     connected = false;
     if(sendInterval != null) {
        clearInterval(sendInterval);
        sendInterval = null;
     }
 });

 client.on('offline', function () {
    debug("in client.on 'offline' function");
     log("MQTT client went offline");
     connected = false;
     if(sendInterval != null) {
        clearInterval(sendInterval);
        sendInterval = null;
     }
 })

if(migrationRegex != null) {
    log("Migrating files matched by regex");
    readMatchingFiles();
}

if(begin) {
    log("Reading from beginning of listned file");
    sendFile(filePath);
}

fs.watchFile(filePath, {interval: 1000}, function (curr, prev) {
    fs.open(filePath, 'r', function (e, fd) {
        if(e) {
            err(e.msg);
        }
        let readSize = null;
        let readFrom = null;
        if(curr.size - prev.size < 0) {
            readSize = curr.size;
            readFrom = 0;
        }
        else {
            readSize = curr.size - prev.size;
            readFrom = prev.size;
        }
        let buffer = Buffer.from(new Uint8Array(readSize));
        fs.readSync(fd, buffer, 0, readSize, readFrom);
        let str = buffer.toString("utf8");
        let lines = str.split('\n');
        lines.forEach(function (line) {
            if(line == "")
                return;
            dataBuffer.push(line);
        });
    });
});

function sendFile(path) {
    debug("in sendFile(path="+path+") function");
    let stringStream = fs.readFileSync(path, {encoding: "utf8"});
    let lines = stringStream.split('\n');
    debug("sending "+lines.length+" lines");
    lines.forEach(function (line) {
        dataBuffer.push(line);
    });
}

function readMatchingFiles() {
    debug("in readMatchingFiles() function");
    glob(migrationRegex, {}, function(e, files) {
        if(e) {
            err(e.msg);
        }
        files.forEach(function (value) {
            debug("matched file: " + value);
            sendFile(value);
        });
    });
}

function debug(msg) {
    if(!debugging)
        return;
    process.stderr.write('[' + new Date().getTime().toString() + "]\t" + "DEBUG: " + msg + '\n');
}

function err(msg, code=1) {
    process.stderr.write('[' + new Date().getTime().toString() + "]\t" + "ERROR: " + msg + '\n');
    process.exit(code);
}

function log(msg) {
    process.stdout.write('[' + new Date().getTime().toString() + "]\t" + "LOG: " + msg + '\n');
}

function warn(msg) {
    process.stdout.write('[' + new Date().getTime().toString() + "]\t" + "WARN: " + msg + '\n');
}

function handleBuffer() {
    debug("in handleBuffer() function");
    if(!connected || sending)
        return;
    sending = true;
    let toSend = dataBuffer;
    dataBuffer = [];
    debug("sending "+Math.ceil(toSend.length/maxObjCount)+" messsages");
    if(toSend.length > 0)
        sendValue(toSend);
}

function sendValue(values) {
    debug("in sendValue function");
    if(!connected) {
        values.forEach(function(val) {
            dataBuffer.push(val);
        });
        return;
    }
    let sndValues = values.slice(0, maxObjCount);
    if(sndValues.length == 0) {
        sending = false;
        return;
    }
    let toSend = values.splice(maxObjCount);
    let sndObj = {};
    sndObj["timestamp"] = new Date().getTime();
    sndObj["data"] = sndValues;
    client.publish(mqttTopic, JSON.stringify(sndObj), {qos: 1}, function (e) {
        if(e) {
            log("MQTT disconected while sending data");
            sendValue(value);
        }
        else {
            setTimeout(function() {
                sendValue(toSend);
            }, 200);
        }
    });
}