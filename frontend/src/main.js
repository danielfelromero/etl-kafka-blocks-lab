import * as Blockly from "blockly";
import "blockly/blocks";
import "./style.css";
import {
  exampleConsumerXml,
  exampleProducerXml,
  registerKafkaBlocks,
  toolbox,
} from "./blocks.js";
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
let currentFilename = "producer_generated.py";
let currentExample = "producer";
let scenario = "basic";

const scenarioDetails = {
  basic: {
    broker: "localhost:9092",
    title: "Modo básico seleccionado · 1 broker",
    instructions: "En Codespaces: ejecuta ./setup_kafka.sh y luego ./diagnose_kafka.sh. La UI no puede comprobar el broker desde el navegador. Para usar Kafbat en modo básico, ejecuta Docker Compose en un host con Docker: docker compose up -d.",
    experiments: "Prueba topic, clave, partición automática/manual, headers, grupos y offsets. Con un broker no se puede demostrar tolerancia a la pérdida de un broker."
  },
  ha: {
    broker: "localhost:19092,localhost:19093,localhost:19094",
    title: "Modo avanzado seleccionado · topología prevista de 3 brokers",
    instructions: "Este selector no inicia el clúster. En un equipo/VM con Docker Compose: docker compose -f docker-compose.ha.yml up -d --wait. Kafbat: http://localhost:8080. Verifica los tres brokers antes de ejecutar productores o consumidores.",
    experiments: "Crea un topic con 3 particiones y replication-factor 3. Compara acks=1 con acks=all y min.insync.replicas=2; observa líderes, réplicas e ISR antes y después de detener un broker."
  }
};

function setBrokerFields(broker) {
  for (const type of ["lab_kafka_setup", "lab_consumer_setup"]) {
    for (const block of workspace.getBlocksByType(type, false)) {
      block.setFieldValue(broker, "BROKER");
    }
  }
}

function renderScenario() {
  const detail = scenarioDetails[scenario];
  document.getElementById("scenario-status").textContent =
    detail.title + " — estado de infraestructura: no verificado.";
  document.getElementById("scenario-instructions").innerHTML =
    "<p><strong>Cómo verificarlo</strong></p><p>" + detail.instructions +
    "</p><p><strong>Experimentos sugeridos</strong></p><p>" + detail.experiments + "</p>";
  setBrokerFields(detail.broker);
  refreshCode();
}

function refreshCode() {
  try {
    codeElement.textContent = generatePython(workspace) || "# Añade bloques para generar código Python.";
  } catch (error) {
    codeElement.textContent = "# No se pudo generar el código: " + error.message;
  }
}

function loadExample(xmlText, filename, exampleName) {
  Blockly.Events.disable();
  try {
    workspace.clear();
    const xml = Blockly.utils.xml.textToDom(xmlText);
    Blockly.Xml.domToWorkspace(xml, workspace);
    currentFilename = filename;
    currentExample = exampleName;
    setBrokerFields(scenarioDetails[scenario].broker);
  } finally {
    Blockly.Events.enable();
  }
  refreshCode();
}

workspace.addChangeListener((event) => {
  if (!event.isUiEvent) refreshCode();
});

document.getElementById("load-producer").addEventListener("click", () => {
  loadExample(exampleProducerXml, "producer_generated.py", "producer");
});
document.getElementById("load-consumer").addEventListener("click", () => {
  loadExample(exampleConsumerXml, "consumer_generated.py", "consumer");
});
document.querySelectorAll('input[name="scenario"]').forEach((input) => {
  input.addEventListener("change", (event) => {
    scenario = event.target.value;
    renderScenario();
  });
});

document.getElementById("copy-code").addEventListener("click", async () => {
  const status = document.getElementById("copy-status");
  try {
    await navigator.clipboard.writeText(generatePython(workspace));
    status.textContent = "Código copiado. Pégalo en un archivo .py del entorno Python para ejecutarlo.";
  } catch {
    status.textContent = "No se pudo usar el portapapeles. Selecciona el código o descarga el archivo.";
  }
});
document.getElementById("download-code").addEventListener("click", () => {
  const blob = new Blob([generatePython(workspace)], { type: "text/x-python;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = currentFilename;
  link.click();
  window.setTimeout(() => URL.revokeObjectURL(url), 1000);
});

renderScenario();
loadExample(exampleProducerXml, currentFilename, currentExample);
