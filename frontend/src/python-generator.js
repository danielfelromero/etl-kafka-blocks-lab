import { pythonGenerator, Order } from "blockly/python";

function escapePythonString(value) {
  return JSON.stringify(String(value));
}

export function registerPythonGenerators() {
  pythonGenerator.forBlock["lab_kafka_setup"] = (block) => {
    const broker = escapePythonString(block.getFieldValue("BROKER"));
    pythonGenerator.definitions_["00_import_datetime"] = "import datetime";
    pythonGenerator.definitions_["01_import_json"] = "import json";
    pythonGenerator.definitions_["02_import_time"] = "import time";
    pythonGenerator.definitions_["03_import_producer"] = "from confluent_kafka import Producer";
    pythonGenerator.definitions_["05_kafka_producer"] =
      "producer = Producer({\"bootstrap.servers\": " + broker + "})";
    return "";
  };

  pythonGenerator.forBlock["lab_event"] = (block) => {
    const station = escapePythonString(block.getFieldValue("STATION"));
    const pm25 = Number(block.getFieldValue("PM25"));
    const temp = Number(block.getFieldValue("TEMP"));
    const expression =
      '{"station_id": ' + station + ', "pm25": ' + pm25 + ', "temperature_c": ' + temp + '}';
    return [expression, Order.ATOMIC];
  };

  pythonGenerator.forBlock["lab_kafka_publish"] = (block, generator) => {
    const event = generator.valueToCode(block, "EVENT", Order.NONE) ||
      '{"station_id": "A01", "pm25": 18.4, "temperature_c": 27.1}';
    const topic = escapePythonString(block.getFieldValue("TOPIC"));
    pythonGenerator.definitions_["04_delivery_callback"] =
      "def delivery_report(error, message):\n" +
      "    if error is not None:\n" +
      "        print(\"Delivery failed: %s\" % error)\n" +
      "    else:\n" +
      "        print(\"Delivered to %s partition %s offset %s\" % (message.topic(), message.partition(), message.offset()))";
    return "event = {**" + event + ', "event_time": datetime.datetime.now(datetime.timezone.utc).isoformat()}\n' +
      "producer.produce(" + topic + ', key=event["station_id"], value=json.dumps(event), callback=delivery_report)\n' +
      "producer.poll(0)\n" +
      'print("Queued:", event)\n';
  };

  pythonGenerator.forBlock["lab_repeat"] = (block, generator) => {
    const count = Math.max(1, Math.min(100, Number(block.getFieldValue("COUNT")) || 1));
    const body = generator.statementToCode(block, "DO");
    return "for _ in range(" + count + "):\n" + (body || "    pass\n");
  };

  pythonGenerator.forBlock["lab_pause"] = (block) => {
    const seconds = Math.max(0, Math.min(60, Number(block.getFieldValue("SECONDS")) || 0));
    return "time.sleep(" + seconds + ")\n";
  };

  pythonGenerator.forBlock["lab_consumer_setup"] = (block) => {
    const broker = escapePythonString(block.getFieldValue("BROKER"));
    const topic = escapePythonString(block.getFieldValue("TOPIC"));
    const group = escapePythonString(block.getFieldValue("GROUP"));
    pythonGenerator.definitions_["00_consumer_import_json"] = "import json";
    pythonGenerator.definitions_["01_consumer_import_client"] = "from confluent_kafka import Consumer, KafkaError";
    return "consumer = Consumer({\n" +
      '    "bootstrap.servers": ' + broker + ",\n" +
      '    "group.id": ' + group + ",\n" +
      '    "auto.offset.reset": "earliest",\n' +
      '    "enable.auto.commit": False,\n' +
      "})\n" +
      "consumer.subscribe([" + topic + "])\n";
  };

  pythonGenerator.forBlock["lab_consumer_loop"] = (block, generator) => {
    let processCode = generator.statementToCode(block, "PROCESS");
    if (!processCode) {
      processCode = '    print("EVENT: %s" % json.dumps(event))\n';
    }
    const nestedProcessCode = processCode
      .split("\n")
      .map((line) => line ? "    " + line : line)
      .join("\n");

    return "try:\n" +
      "    while True:\n" +
      "        message = consumer.poll(1.0)\n" +
      "        if message is None:\n" +
      "            continue\n" +
      "        if message.error():\n" +
      "            if message.error().code() == KafkaError._PARTITION_EOF:\n" +
      "                continue\n" +
      '            print("Kafka error: %s" % message.error())\n' +
      "            continue\n" +
      "        try:\n" +
      '            event = json.loads(message.value().decode("utf-8"))\n' +
      "        except (UnicodeDecodeError, json.JSONDecodeError) as error:\n" +
      '            print("Rejected invalid JSON: %s" % error)\n' +
      "            consumer.commit(message=message, asynchronous=False)\n" +
      "            continue\n" +
      "        if not isinstance(event, dict):\n" +
      '            print("REJECTED: event must be a JSON object")\n' +
      "            consumer.commit(message=message, asynchronous=False)\n" +
      "            continue\n" +
      nestedProcessCode +
      "        consumer.commit(message=message, asynchronous=False)\n" +
      "except KeyboardInterrupt:\n" +
      '    print("Stopping consumer...")\n' +
      "finally:\n" +
      "    consumer.close()\n";
  };

  pythonGenerator.forBlock["lab_consumer_validate"] = () => {
    return "if not isinstance(event, dict):\n" +
      '    print("REJECTED: event must be a JSON object")\n' +
      "else:\n" +
      '    required = ("station_id", "pm25", "event_time")\n' +
      "    missing = [field for field in required if field not in event]\n" +
      "    if missing:\n" +
      '        print("REJECTED: missing fields %s" % missing)\n' +
      "    else:\n" +
      "        try:\n" +
      '            pm25 = float(event["pm25"])\n' +
      "            if pm25 < 0:\n" +
      '                raise ValueError("pm25 cannot be negative")\n' +
      '            event["pm25"] = pm25\n' +
      '            event["pm25_category"] = "high" if pm25 >= 25 else "normal"\n' +
      '            print("PROCESSED: %s" % json.dumps(event))\n' +
      "        except (TypeError, ValueError) as error:\n" +
      '            print("REJECTED: %s" % error)\n';
  };

  pythonGenerator.forBlock["lab_consumer_display"] = () => {
    return 'print("EVENT: %s" % json.dumps(event))\n';
  };
}

export function generatePython(workspace) {
  const body = pythonGenerator.workspaceToCode(workspace);
  const prefix = '"""Generated by ETL Kafka Blocks Lab. Educational example."""\n\n' +
    "# The Kafka broker must be running before executing this file.\n\n";
  const code = prefix + body;
  return code.includes("producer.produce(") ? code + "\nproducer.flush(10)\n" : code;
}
