import { pythonGenerator, Order } from "blockly/python";

const pyString = (value) => JSON.stringify(String(value));
const safeIdentifier = (value) => String(value).replace(/[^a-zA-Z0-9_]/g, "_");

export function registerPythonGenerators() {
  pythonGenerator.forBlock["lab_kafka_setup"] = (block, generator) => {
    const broker = pyString(block.getFieldValue("BROKER"));
    pythonGenerator.definitions_["00_import_json"] = "import json";
    pythonGenerator.definitions_["01_import_time"] = "import time";
    pythonGenerator.definitions_["02_import_kafka"] = "from confluent_kafka import Producer";
    pythonGenerator.definitions_["03_import_datetime"] = "import datetime";
    pythonGenerator.definitions_["04_delivery_callback"] =
      "def delivery_report(error, message):\n" +
      "    if error is not None:\n" +
      "        print('Delivery failed: %s' % error)\n" +
      "    else:\n" +
      "        print('Delivered topic=%s partition=%s offset=%s' % (message.topic(), message.partition(), message.offset()))";
    const options = generator.valueToCode(block, "OPTIONS", Order.NONE);
    pythonGenerator.definitions_["05_producer"] =
      "producer = Producer({\"bootstrap.servers\": " + broker + (options ? ", **" + options : "") + "})";
    return "";
  };

  pythonGenerator.forBlock["lab_kafka_options"] = (block) => {
    const acks = pyString(block.getFieldValue("ACKS") || "all");
    const retries = Math.max(0, Number(block.getFieldValue("RETRIES")) || 0);
    const linger = Math.max(0, Number(block.getFieldValue("LINGER")) || 0);
    const compression = pyString(block.getFieldValue("COMPRESSION") || "none");
    const idempotence = block.getFieldValue("IDEMPOTENCE") === "true" ? "True" : "False";
    const expression = '{"acks": ' + acks +
      ', "retries": ' + retries +
      ', "linger.ms": ' + linger +
      ', "compression.type": ' + compression +
      ', "enable.idempotence": ' + idempotence + "}";
    return [expression, Order.ATOMIC];
  };
  pythonGenerator.forBlock["lab_event"] = (block) => {
    const raw = block.getFieldValue("JSON") || "{}";
    try {
      JSON.parse(raw);
    } catch {
      return ["raise ValueError(" + pyString("JSON inválido en el bloque Evento: corrige la sintaxis antes de publicar.") + ")", Order.ATOMIC];
    }
    return ["json.loads(" + pyString(raw) + ")", Order.ATOMIC];
  };

  pythonGenerator.forBlock["lab_header"] = (block) => {
    const name = pyString(block.getFieldValue("NAME"));
    const value = pyString(block.getFieldValue("VALUE"));
    return "(" + name + ", " + value + "),\n";
  };

  pythonGenerator.forBlock["lab_kafka_publish"] = (block, generator) => {
    const event = generator.valueToCode(block, "EVENT", Order.NONE) || "json.loads('{}')";
    const topic = pyString(block.getFieldValue("TOPIC"));
    const key = block.getFieldValue("KEY");
    const mode = block.getFieldValue("PARTITION_MODE");
    const partition = Math.max(0, Number(block.getFieldValue("PARTITION")) || 0);
    const headersCode = generator.statementToCode(block, "HEADERS");
    const headers = headersCode.trim() ? "[" + headersCode.trim().replace(/,?\s*$/, "") + "]" : "None";
    const keyExpr = key === "" ? "None" : pyString(key);
    const partitionArg = mode === "MANUAL" ? ", partition=" + partition : "";
    const varName = "event_" + safeIdentifier(block.id);
    return varName + " = " + event + "\n" +
      "if not isinstance(" + varName + ", dict):\n" +
      "    raise ValueError('El evento debe ser un objeto JSON')\n" +
      varName + '["event_time"] = datetime.datetime.now(datetime.timezone.utc).isoformat()\n' +
      "producer.produce(topic=" + topic + ", key=" + keyExpr + ", value=json.dumps(" + varName + "), headers=" + headers + partitionArg + ", callback=delivery_report)\n" +
      "producer.poll(0)\n" +
      "print('Enviado a ' + " + topic + " + ': ' + json.dumps(" + varName + "))\n";
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
    const broker = pyString(block.getFieldValue("BROKER"));
    const topics = String(block.getFieldValue("TOPICS") || "").split(",").map(x => x.trim()).filter(Boolean);
    const group = pyString(block.getFieldValue("GROUP"));
    const offset = block.getFieldValue("OFFSET") || "earliest";
    pythonGenerator.definitions_["00_consumer_imports"] = "import json\nfrom confluent_kafka import Consumer, KafkaError";
    return "consumer = Consumer({\n" +
      '    "bootstrap.servers": ' + broker + ",\n" +
      '    "group.id": ' + group + ",\n" +
      '    "auto.offset.reset": ' + pyString(offset) + ",\n" +
      '    "enable.auto.commit": False,\n' +
      "})\n" +
      "consumer.subscribe(" + JSON.stringify(topics) + ")\n";
  };

  pythonGenerator.forBlock["lab_consumer_loop"] = (block, generator) => {
    let processCode = generator.statementToCode(block, "PROCESS");
    if (!processCode) processCode = 'print("EVENT:", event)\n';
    const nested = processCode.split("\n").map(line => line.trim() ? "            " + line.trimStart() : line).join("\n");
    return "try:\n" +
      "    while True:\n" +
      "        message = consumer.poll(1.0)\n" +
      "        if message is None:\n            continue\n" +
      "        if message.error():\n" +
      "            if message.error().code() == KafkaError._PARTITION_EOF:\n                continue\n" +
      '            print("Kafka error:", message.error())\n            continue\n' +
      "        try:\n            event = json.loads(message.value().decode('utf-8'))\n" +
      "        except (UnicodeDecodeError, json.JSONDecodeError) as error:\n" +
      '            print("Mensaje JSON inválido:", error)\n            consumer.commit(message=message, asynchronous=False)\n            continue\n' +
      "        if not isinstance(event, dict):\n" +
      '            print("El valor JSON no es un objeto")\n            consumer.commit(message=message, asynchronous=False)\n            continue\n' +
      nested +
      "        consumer.commit(message=message, asynchronous=False)\n" +
      "except KeyboardInterrupt:\n    print('Deteniendo consumidor...')\n" +
      "finally:\n    consumer.close()\n";
  };

  pythonGenerator.forBlock["lab_consumer_inspect"] = () =>
    "print({\n" +
    "    'topic': message.topic(),\n" +
    "    'partition': message.partition(),\n" +
    "    'offset': message.offset(),\n" +
    "    'key': message.key().decode('utf-8') if message.key() else None,\n" +
    "    'headers': message.headers(),\n" +
    "    'event': event,\n" +
    "})\n";

  pythonGenerator.forBlock["lab_consumer_validate"] = () =>
    "required = ('station_id', 'pm25', 'event_time')\n" +
    "missing = [field for field in required if field not in event]\n" +
    "if missing:\n    print('REJECTED missing fields:', missing)\n" +
    "else:\n" +
    "    try:\n        pm25 = float(event['pm25'])\n" +
    "        if pm25 < 0:\n            raise ValueError('pm25 cannot be negative')\n" +
    "        event['pm25'] = pm25\n" +
    "        event['pm25_category'] = 'high' if pm25 >= 25 else 'normal'\n" +
    "        print('PROCESSED:', event)\n" +
    "    except (TypeError, ValueError) as error:\n        print('REJECTED:', error)\n";

  pythonGenerator.forBlock["lab_consumer_display"] = () => "print('EVENT JSON:', json.dumps(event, ensure_ascii=False))\n";
}

export function generatePython(workspace) {
  return '"""Generated by ETL Kafka Blocks Lab. Educational example."""\n\n' +
    "# Kafka must be running before executing this file.\n\n" +
    pythonGenerator.workspaceToCode(workspace);
}
