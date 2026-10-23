import { spawnSync } from "node:child_process";
import * as Blockly from "blockly/core";
import { Workspace } from "blockly/core";
import { registerKafkaBlocks } from "../src/blocks.js";
import { generatePython, registerPythonGenerators } from "../src/python-generator.js";

registerKafkaBlocks();
registerPythonGenerators();

function connect(parent, child) {
  if (!parent.nextConnection || !child.previousConnection) {
    throw new Error("Missing statement connection when building a sample workspace");
  }
  parent.nextConnection.connect(child.previousConnection);
}

function assertPythonSyntax(name, code) {
  const result = spawnSync(
    "python",
    ["-c", "import ast,sys; ast.parse(sys.stdin.read())"],
    { input: code, encoding: "utf8" },
  );
  if (result.status !== 0) {
    throw new Error(name + " generated invalid Python:\n" + result.stderr + "\nGenerated code:\n" + code);
  }
  console.log("Valid Python:", name);
}

function producerExample() {
  const workspace = new Workspace();
  const setup = workspace.newBlock("lab_kafka_setup");
  const repeat = workspace.newBlock("lab_repeat");
  const publish = workspace.newBlock("lab_kafka_publish");
  const event = workspace.newBlock("lab_event");
  const pause = workspace.newBlock("lab_pause");

  setup.setFieldValue("kafka:29092", "BROKER");
  repeat.setFieldValue("3", "COUNT");
  publish.setFieldValue("mediciones_aire", "TOPIC");
  event.setFieldValue("A02", "STATION");
  event.setFieldValue("22.5", "PM25");
  event.setFieldValue("28.0", "TEMP");

  connect(setup, repeat);
  repeat.getInput("DO").connection.connect(publish.previousConnection);
  publish.getInput("EVENT").connection.connect(event.outputConnection);
  connect(publish, pause);

  const code = generatePython(workspace);
  if (!code.includes("producer.produce(") || !code.includes('"station_id": "A02"')) {
    throw new Error("Producer example did not generate expected Kafka and event code:\n" + code);
  }
  assertPythonSyntax("producer", code);
  workspace.dispose();
}

function consumerExample() {
  const workspace = new Workspace();
  const setup = workspace.newBlock("lab_consumer_setup");
  const loop = workspace.newBlock("lab_consumer_loop");
  const validate = workspace.newBlock("lab_consumer_validate");

  setup.setFieldValue("kafka:29092", "BROKER");
  setup.setFieldValue("mediciones_aire", "TOPIC");
  setup.setFieldValue("etl-kafka-blocks-demo", "GROUP");

  connect(setup, loop);
  loop.getInput("PROCESS").connection.connect(validate.previousConnection);

  const code = generatePython(workspace);
  if (!code.includes("consumer.poll(") || !code.includes("pm25_category")) {
    throw new Error("Consumer example did not generate expected read/transform code:\n" + code);
  }
  assertPythonSyntax("consumer", code);
  workspace.dispose();
}

producerExample();
consumerExample();
