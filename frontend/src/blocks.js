import * as Blockly from "blockly/core";

export function registerKafkaBlocks() {
  const definitions = {
    lab_kafka_setup: {
    init() {
      this.appendDummyInput()
        .appendField("Configurar productor Kafka")
        .appendField("broker")
        .appendField(new Blockly.FieldTextInput("kafka:29092"), "BROKER");
      this.setNextStatement(true);
      this.setColour(210);
      this.setTooltip("Define el broker al que se conectará el productor.");
      this.setHelpUrl("https://docs.confluent.io/kafka-clients/python/current/overview.html");
    },
    },

    lab_event: {
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
    },

    lab_kafka_publish: {
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
    },

    lab_repeat: {
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
    },

    lab_pause: {
    init() {
      this.appendDummyInput().appendField("Esperar")
        .appendField(new Blockly.FieldNumber(1, 0, 60, 0.5), "SECONDS")
        .appendField("segundos");
      this.setPreviousStatement(true);
      this.setNextStatement(true);
      this.setColour(35);
    },
    },

    lab_consumer_setup: {
    init() {
      this.appendDummyInput().appendField("Configurar consumidor Kafka")
        .appendField("broker")
        .appendField(new Blockly.FieldTextInput("kafka:29092"), "BROKER");
      this.appendDummyInput().appendField("tópico")
        .appendField(new Blockly.FieldTextInput("mediciones_aire"), "TOPIC");
      this.appendDummyInput().appendField("grupo")
        .appendField(new Blockly.FieldTextInput("etl-kafka-blocks-demo"), "GROUP");
      this.setNextStatement(true);
      this.setColour(275);
      this.setTooltip("Configura el cliente consumidor, el tópico y el grupo.");
    },
    },

    lab_consumer_loop: {
    init() {
      this.appendDummyInput().appendField("Leer eventos continuamente");
      this.appendStatementInput("PROCESS").appendField("al procesar cada evento");
      this.setPreviousStatement(true);
      this.setColour(275);
      this.setTooltip("Genera el ciclo de lectura, manejo de errores y cierre del consumidor.");
    },
    },

    lab_consumer_validate: {
    init() {
      this.appendDummyInput().appendField("Validar PM2.5 y clasificar");
      this.setPreviousStatement(true);
      this.setNextStatement(true);
      this.setColour(120);
      this.setTooltip("Rechaza campos obligatorios ausentes o valores de PM2.5 negativos; clasifica los registros válidos.");
    },
    },

    lab_consumer_display: {
    init() {
      this.appendDummyInput().appendField("Solo mostrar evento JSON");
      this.setPreviousStatement(true);
      this.setNextStatement(true);
      this.setColour(120);
      this.setTooltip("Imprime cada evento recibido sin transformarlo.");
    },
    },
  };
  Blockly.common.defineBlocks(definitions);
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
      { kind: "block", type: "lab_consumer_setup" },
      { kind: "block", type: "lab_consumer_loop" },
      { kind: "block", type: "lab_consumer_validate" },
      { kind: "block", type: "lab_consumer_display" },
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
  '  <block type="lab_consumer_setup" x="35" y="30">',
  '    <field name="BROKER">kafka:29092</field>',
  '    <field name="TOPIC">mediciones_aire</field>',
  '    <field name="GROUP">etl-kafka-blocks-demo</field>',
  '    <next>',
  '      <block type="lab_consumer_loop">',
  '        <statement name="PROCESS"><block type="lab_consumer_validate"></block></statement>',
  '      </block>',
  '    </next>',
  '  </block>',
  '</xml>',
].join("\n");
