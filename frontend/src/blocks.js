import * as Blockly from "blockly/core";

export function registerKafkaBlocks() {
  Blockly.common.defineBlocks({
    lab_kafka_setup: {
      init() {
        this.appendDummyInput().appendField("1 · Configurar productor")
          .appendField("broker").appendField(new Blockly.FieldTextInput("localhost:9092"), "BROKER");
        this.setNextStatement(true); this.setColour(210);
        this.setTooltip("Define a qué broker se conectará el productor.");
      },
    },
    lab_event: {
      init() {
        this.appendDummyInput().appendField("2 · Crear evento JSON");
        this.appendDummyInput().appendField("contenido JSON")
          .appendField(new Blockly.FieldTextInput('{"station_id":"A01","pm25":18.4,"temperature_c":27.1}'), "JSON");
        this.setOutput(true, "Event"); this.setColour(155);
        this.setTooltip("El valor del mensaje. Escribe JSON válido: cadenas entre comillas, números sin comillas.");
      },
    },
    lab_header: {
      init() {
        this.appendDummyInput().appendField("Header Kafka")
          .appendField("nombre").appendField(new Blockly.FieldTextInput("source"), "NAME")
          .appendField("valor").appendField(new Blockly.FieldTextInput("sensor"), "VALUE");
        this.setPreviousStatement(true); this.setNextStatement(true); this.setColour(65);
        this.setTooltip("Añade metadatos clave-valor al mensaje. Los headers no forman parte del JSON del evento.");
      },
    },
    lab_kafka_publish: {
      init() {
        this.appendValueInput("EVENT").setCheck("Event").appendField("3 · Enviar mensaje");
        this.appendDummyInput().appendField("tópico").appendField(new Blockly.FieldTextInput("mediciones_aire"), "TOPIC");
        this.appendDummyInput().appendField("clave (vacía = ninguna)").appendField(new Blockly.FieldTextInput(""), "KEY");
        this.appendDummyInput().appendField("partición")
          .appendField(new Blockly.FieldDropdown([["Kafka la asigna", "AUTO"], ["Elegir manualmente", "MANUAL"]]), "PARTITION_MODE")
          .appendField("n.º").appendField(new Blockly.FieldNumber(0, 0, 99, 1), "PARTITION");
        this.appendStatementInput("HEADERS").appendField("headers (opcionales)");
        this.setPreviousStatement(true); this.setNextStatement(true); this.setColour(210);
        this.setTooltip("Cada bloque Enviar mensaje puede apuntar a un tópico distinto. La partición manual debe existir en el tópico.");
      },
    },
    lab_repeat: {
      init() {
        this.appendDummyInput().appendField("Repetir").appendField(new Blockly.FieldNumber(5, 1, 100, 1), "COUNT").appendField("veces");
        this.appendStatementInput("DO").appendField("hacer"); this.setPreviousStatement(true); this.setNextStatement(true); this.setColour(35);
      },
    },
    lab_pause: {
      init() {
        this.appendDummyInput().appendField("Esperar").appendField(new Blockly.FieldNumber(1, 0, 60, 0.5), "SECONDS").appendField("segundos");
        this.setPreviousStatement(true); this.setNextStatement(true); this.setColour(35);
      },
    },
    lab_consumer_setup: {
      init() {
        this.appendDummyInput().appendField("1 · Configurar consumidor").appendField("broker")
          .appendField(new Blockly.FieldTextInput("localhost:9092"), "BROKER");
        this.appendDummyInput().appendField("tópicos (separados por coma)")
          .appendField(new Blockly.FieldTextInput("mediciones_aire"), "TOPICS");
        this.appendDummyInput().appendField("grupo")
          .appendField(new Blockly.FieldTextInput("etl-kafka-blocks-demo"), "GROUP");
        this.appendDummyInput().appendField("offset inicial")
          .appendField(new Blockly.FieldDropdown([["desde el inicio", "earliest"], ["solo mensajes nuevos", "latest"]]), "OFFSET");
        this.setNextStatement(true); this.setColour(275);
        this.setTooltip("El consumidor puede suscribirse a uno o varios tópicos.");
      },
    },
    lab_consumer_loop: {
      init() {
        this.appendDummyInput().appendField("2 · Leer mensajes continuamente");
        this.appendStatementInput("PROCESS").appendField("3 · procesar cada mensaje");
        this.setPreviousStatement(true); this.setColour(275);
      },
    },
    lab_consumer_inspect: {
      init() {
        this.appendDummyInput().appendField("Mostrar tópico, partición, offset, clave, headers y valor");
        this.setPreviousStatement(true); this.setNextStatement(true); this.setColour(120);
        this.setTooltip("Inspecciona los metadatos Kafka y el contenido del evento.");
      },
    },
    lab_consumer_validate: {
      init() {
        this.appendDummyInput().appendField("Validar PM2.5 y clasificar");
        this.setPreviousStatement(true); this.setNextStatement(true); this.setColour(120);
        this.setTooltip("Valida campos obligatorios y clasifica el valor PM2.5.");
      },
    },
    lab_consumer_display: {
      init() {
        this.appendDummyInput().appendField("Mostrar solo el JSON del evento");
        this.setPreviousStatement(true); this.setNextStatement(true); this.setColour(120);
      },
    },
  });
}

export const toolbox = {
  kind: "categoryToolbox",
  contents: [
    { kind: "category", name: "Productor", colour: "210", contents: [
      { kind: "block", type: "lab_kafka_setup" }, { kind: "block", type: "lab_kafka_publish" },
      { kind: "block", type: "lab_repeat" }, { kind: "block", type: "lab_pause" },
    ]},
    { kind: "category", name: "Evento y metadatos", colour: "155", contents: [
      { kind: "block", type: "lab_event" }, { kind: "block", type: "lab_header" },
    ]},
    { kind: "category", name: "Consumidor", colour: "275", contents: [
      { kind: "block", type: "lab_consumer_setup" }, { kind: "block", type: "lab_consumer_loop" },
      { kind: "block", type: "lab_consumer_inspect" }, { kind: "block", type: "lab_consumer_validate" },
      { kind: "block", type: "lab_consumer_display" },
    ]},
  ],
};

export const exampleProducerXml = [
  '<xml xmlns="https://developers.google.com/blockly/xml">',
  ' <block type="lab_kafka_setup" x="25" y="25"><next>',
  '  <block type="lab_repeat"><field name="COUNT">3</field><statement name="DO">',
  '   <block type="lab_kafka_publish"><field name="TOPIC">mediciones_aire</field><field name="KEY">A01</field><field name="PARTITION_MODE">AUTO</field>',
  '    <value name="EVENT"><block type="lab_event"><field name="JSON">{"station_id":"A01","pm25":18.4,"temperature_c":27.1}</field></block></value>',
  '    <statement name="HEADERS"><block type="lab_header"><field name="NAME">source</field><field name="VALUE">sensor</field></block></statement>',
  '    <next><block type="lab_pause"><field name="SECONDS">1</field></block></next>',
  '   </block></statement></block>',
  ' </next></block>',
  '</xml>',
].join("\n");

export const exampleConsumerXml = [
  '<xml xmlns="https://developers.google.com/blockly/xml">',
  ' <block type="lab_consumer_setup" x="25" y="25"><field name="TOPICS">mediciones_aire, alertas_aire</field>',
  '  <next><block type="lab_consumer_loop">',
  '   <statement name="PROCESS"><block type="lab_consumer_inspect"></block></statement>',
  '  </block></next>',
  ' </block>',
  '</xml>',
].join("\n");
