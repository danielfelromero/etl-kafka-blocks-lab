import * as Blockly from "blockly/core";
import * as libraryBlocks from "blockly/blocks";
import "./style.css";
import {
  exampleConsumerXml,
  exampleProducerXml,
  registerKafkaBlocks,
  toolbox,
} from "./blocks.js";
import { generatePython, registerPythonGenerators } from "./python-generator.js";

Blockly.common.defineBlocks(libraryBlocks);
registerKafkaBlocks();
registerPythonGenerators();

const workspace = Blockly.inject("blockly-workspace", {
  toolbox,
  trashcan: true,
  renderer: "zelos",
  grid: { spacing: 20, length: 3, colour: "#d8e1ee", snap: true },
  zoom: { controls: true, wheel: true, startScale: 0.9, maxScale: 1.6, minScale: 0.5 },
  move: { scrollbars: true, drag: true, wheel: true },
});

const codeElement = document.getElementById("python-code");
let currentFilename = "producer_generated.py";

function refreshCode() {
  try {
    codeElement.textContent = generatePython(workspace) || "# Añade bloques para generar código Python.";
  } catch (error) {
    codeElement.textContent = "# No se pudo generar el código: " + error.message;
  }
}

function loadExample(xmlText, filename) {
  Blockly.Events.disable();
  try {
    workspace.clear();
    const xml = Blockly.utils.xml.textToDom(xmlText);
    Blockly.Xml.domToWorkspace(xml, workspace);
    currentFilename = filename;
  } finally {
    Blockly.Events.enable();
  }
  refreshCode();
}

workspace.addChangeListener((event) => {
  if (!event.isUiEvent) refreshCode();
});

document.getElementById("load-producer").addEventListener("click", () => {
  loadExample(exampleProducerXml, "producer_generated.py");
});

document.getElementById("load-consumer").addEventListener("click", () => {
  loadExample(exampleConsumerXml, "consumer_generated.py");
});

document.getElementById("copy-code").addEventListener("click", async () => {
  const status = document.getElementById("copy-status");
  try {
    await navigator.clipboard.writeText(generatePython(workspace));
    status.textContent = "Código copiado. Pégalo en un archivo .py del Codespace para ejecutarlo.";
  } catch (error) {
    status.textContent = "No se pudo usar el portapapeles. Puedes seleccionar el código o descargar el archivo.";
  }
});

document.getElementById("download-code").addEventListener("click", () => {
  const code = generatePython(workspace);
  const blob = new Blob([code], { type: "text/x-python;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = currentFilename;
  link.click();
  window.setTimeout(() => URL.revokeObjectURL(url), 1000);
});

loadExample(exampleProducerXml, currentFilename);
