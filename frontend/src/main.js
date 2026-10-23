import * as Blockly from "blockly";
import "blockly/blocks";
import "./style.css";
import { exampleXml, registerKafkaBlocks, toolbox } from "./blocks.js";
import { generatePython, registerPythonGenerators } from "./python-generator.js";

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

function refreshCode() {
  try {
    codeElement.textContent = generatePython(workspace) || "# Añade bloques para generar código Python.";
  } catch (error) {
    codeElement.textContent = `# No se pudo generar el código: ${error.message}`;
  }
}

workspace.addChangeListener((event) => {
  if (!event.isUiEvent) refreshCode();
});

document.getElementById("load-example").addEventListener("click", () => {
  Blockly.Events.disable();
  try {
    workspace.clear();
    const xml = Blockly.utils.xml.textToDom(exampleXml);
    Blockly.Xml.domToWorkspace(xml, workspace);
  } finally {
    Blockly.Events.enable();
  }
  refreshCode();
});

document.getElementById("download-code").addEventListener("click", () => {
  const code = generatePython(workspace);
  const blob = new Blob([code], { type: "text/x-python;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = "producer_generated.py";
  link.click();
  URL.revokeObjectURL(url);
});

document.getElementById("load-example").click();
refreshCode();
