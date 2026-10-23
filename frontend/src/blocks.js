import * as Blockly from "blockly/core";

export function registerKafkaBlocks() {
  Blockly.Blocks["lab_kafka_setup"] = {
    init() {
      this.appendDummyInput()
        .appendField("Configurar productor Kafka")
        .appendField("broker")
        .appendField(new Blockly.FieldTextInput("localhost:9092"), "BROKER");
      this.setNextStatement(true);
      this.setColour(210);
      this.setTooltip("Define el broker al que se conectará el productor.");
      this.setHelpUrl("https://docs.confluent.io/kafka-clients/python/current/overview.html");
    },
  };

  Blockly.Blocks["lab_event"] = {
    init() {
      this.appendDummyInput().appendField("Crear medición")
        .appendField("estación")
        .appendField(new Blockly.FieldTextInput("A01"), "STATION");
      this.appendDummyInput().appendField("PM2.5")
        .appendField(new Blockly.FieldNumber(18.4, -100, 1000, 0.1), "PM25");
      this.appendDummyInput().appendField("temperatura °C")
        .appendField(new Blockly.FieldNumber(27.1, -50, 80, 0.1), "TEMP");
      this.setOutput(true, "Event");
      this.setColour(155);
      this.setTooltip("Construye el objeto que se enviará como evento JSON.");
    },
  };

  Blockly.Blocks["lab_kafka_publish"] = {
    init() {
      this.appendValueInput("EVENT").setCheck("Event").appendField("Publicar evento en tópico");
      this.appendDummyInput().appendField("tópico")
        .appendField(new Blockly.FieldTextInput("mediciones_aire"), "TOPIC");
      this.appendDummyInput().appendField("clave del mensaje = station_id");
      this.setPreviousStatement(true);
      this.setNextStatement(true);
      this.setColour(210);
      this.setTooltip("Publica el evento como JSON y utiliza la estación como clave.");
    },
  };

  Blockly.Blocks["lab_repeat"] = {
    init() {
      this.appendDummyInput().appendField("Repetir")
        .appendField(new Blockly.FieldNumber(5, 1, 100, 1), "COUNT")
        .appendField("veces");
      this.appendStatementInput("DO").appendField("hacer");
      this.setPreviousStatement(true);
      this.setNextStatement(true);
      this.setColour(35);
      this.setTooltip("Repite los bloques conectados debajo.");
    },
  };

  Blockly.Blocks["lab_pause"] = {
    init() {
      this.appendDummyInput().appendField("Esperar")
        .appendField(new Blockly.FieldNumber(1, 0, 60, 0.5), "SECONDS")
        .appendField("segundos");
      this.setPreviousStatement(true);
      this.setNextStatement(true);
      this.setColour(35);
    },
  };

  Blockly.Blocks["lab_consumer_program"] = {
    init() {
      this.appendDummyInput().appendField("Programa completo: consumidor Kafka");
      this.appendDummyInput().appendField("broker")
        .appendField(new Blockly.FieldTextInput("localhost:9092"), "BROKER");
      this.appendDummyInput().appendField("tópico")
        .appendField(new Blockly.FieldTextInput("mediciones_aire"), "TOPIC");
      this.appendDummyInput().appendField("grupo")
        .appendField(new Blockly.FieldTextInput("etl-kafka-blocks-demo"), "GROUP");
      this.appendDummyInput().appendField("procesamiento")
        .appendField(new Blockly.FieldDropdown([
          ["Validar PM2.5 y clasificar", "validate"],
          ["Solo mostrar eventos", "display"],
        ]), "STRATEGY");
      this.setPreviousStatement(true);
      this.setColour(275);
      this.setTooltip("Genera un consumidor Python ejecutable, con lectura, cierre y procesamiento básico.");
      this.setHelpUrl("https://docs.confluent.io/kafka-clients/python/current/overview.html");
    },
  };
}

export const toolbox = {
  kind: "categoryToolbox",
  contents: [
    { kind: "category", name: "Productor", colour: "210", contents: [
      { kind: "block", type: "lab_kafka_setup" },
      { kind: "block", type: "lab_kafka_publish" },
    ]},
    { kind: "category", name: "Datos", colour: "155", contents: [
      { kind: "block", type: "lab_event" },
    ]},
    { kind: "category", name: "Control", colour: "35", contents: [
      { kind: "block", type: "lab_repeat" },
      { kind: "block", type: "lab_pause" },
    ]},
    { kind: "category", name: "Consumidor", colour: "275", contents: [
      { kind: "block", type: "lab_consumer_program" },
    ]},
    { kind: "category", name: "Lógica", categorystyle: "logic_category", contents: [
      { kind: "block", type: "controls_if" },
      { kind: "block", type: "logic_compare" },
      { kind: "block", type: "logic_boolean" },
    ]},
    { kind: "category", name: "Variables", custom: "VARIABLE", categorystyle: "variable_category" },
  ],
};

export const exampleProducerXml = [
  '<xml xmlns="https://developers.google.com/blockly/xml">',
  '  <block type="lab_kafka_setup" x="35" y="30">',
  '    <next><block type="lab_repeat"><field name="COUNT">5</field>',
  '      <statement name="DO"><block type="lab_kafka_publish"><field name="TOPIC">mediciones_aire</field>',
  '        <value name="EVENT"><block type="lab_event"><field name="STATION">A01</field>',
  '          <field name="PM25">18.4</field><field name="TEMP">27.1</field></block></value>',
  '        <next><block type="lab_pause"><field name="SECONDS">1</field></block></next>',
  '      </block></statement></block></next>',
  '  </block>',
  '</xml>',
].join("\n");

export const exampleConsumerXml = [
  '<xml xmlns="https://developers.google.com/blockly/xml">',
  '  <block type="lab_consumer_program" x="35" y="30">',
  '    <field name="BROKER">localhost:9092</field>',
  '    <field name="TOPIC">mediciones_aire</field>',
  '    <field name="GROUP">etl-kafka-blocks-demo</field>',
  '    <field name="STRATEGY">validate</field>',
  '  </block>',
  '</xml>',
].join("\n");
