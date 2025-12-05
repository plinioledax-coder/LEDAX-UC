// =========================
// script.js — Versão sem Heatmap e sem Cidades
// =========================

// CONFIG
const API_BASE = "https://ledax-uc.onrender.com/";
const ENDPOINT_REDES = `${API_BASE}/unidades/redes`;
const ENDPOINT_ALL = `${API_BASE}/unidades/all`;
const GEOJSON_PATH = "brasil_estados.geojson";

// ESTADO GLOBAL
let map;
let unidades = [];
let unidadesAtuais = [];
let clusterGroup = L.markerClusterGroup();
let markersLayer = L.layerGroup();
let clustersAtivos = true;

// Cores por rede
const REDE_COLORS = {
  "Novo Mix": '#1f77b4',
  "Hiperideal": '#ff7f0e',
  "Rede Mix": '#9467bd',
  "DEFAULT": '#2ca02c'
};

// NOVO: Para guardar referência das camadas dos representantes
const camadaRepAtiva = {};

// MAPA DE COBERTURA DOS REPRESENTANTES (UFs)
const coberturaRepresentante = {
  "RENATO PEREIRA": ["MT"],
  "RODRIGO LISBOA": ["MG"],
  "DANIEL DE EQUIP.": ["PE", "RN"],
  "CLECIO SALVIANO": ["SP"],
  "HAMILTON MORAES": ["GO", "MS"],
  "MARCOS BARIANI": ["SP", "AL"],
  "ALEXANDRE CÂND.": ["AL", "DF"],
  "EDSEU MARQUES": ["TO", "PA"],
  "MAURO FOLLMANN": ["PA"],
  "MANOEL AFONSO": ["AC", "RO"],
  "JOSÉ LOBO": ["BA"],
  "PEDRO AMORIM": ["RJ"],
  "CRYSTIANO SILVA": ["AM"],
  "ERNESTO (LLAMPE)": ["SC", "PR"],
  "SEM COBERTURA": "RESTO"
};

// CORES DE CADA REPRESENTANTE
const coresRepresentante = {
  "RENATO PEREIRA": "rgba(255, 105, 180, 0.7)",
  "RODRIGO LISBOA": "rgba(0, 191, 255, 0.7)",
  "DANIEL DE EQUIP.": "rgba(255, 69, 0, 0.7)",
  "CLECIO SALVIANO": "rgba(147, 112, 219, 0.7)",
  "HAMILTON MORAES": "rgba(255, 215, 0, 0.7)",
  "MARCOS BARIANI": "rgba(0, 255, 255, 0.7)",
  "ALEXANDRE CÂND.": "rgba(255, 165, 0, 0.7)",
  "EDSEU MARQUES": "rgba(128, 0, 128, 0.7)",
  "MAURO FOLLMANN": "rgba(255, 0, 0, 0.7)",
  "MANOEL AFONSO": "rgba(0, 128, 0, 0.7)",
  "JOSÉ LOBO": "rgba(255, 140, 0, 0.7)",
  "PEDRO AMORIM": "rgba(70, 130, 180, 0.7)",
  "CRYSTIANO SILVA": "rgba(0, 0, 255, 0.7)",
  "ERNESTO (LLAMPE)": "rgba(0, 128, 15, 0.58)",
  "SEM COBERTURA": "rgba(180, 180, 180, 0.55)"
};

// Cor quando dois reps cobrem o mesmo estado
const COR_SOBREPOSICAO = "rgba(255, 102, 0, 0.8)";

// -------------------------
// UTILITÁRIOS
// -------------------------
function safeText(s){ return (s || '').toString(); }
function normalizeString(s){ return safeText(s).trim().toLowerCase(); }

function renderCoberturaRepresentantes() {
    const cont = document.getElementById("cobertura-legend");
    cont.innerHTML = "";

    Object.keys(coberturaRepresentante).forEach(rep => {
        const safeId = `rep-${rep.replace(/\s+/g, '-')}`;
        const color = coresRepresentante[rep] || "#999";

        const row = document.createElement("div");
        row.className = "checkbox-row";

        row.innerHTML = `
            <input type="checkbox" id="${safeId}" value="${rep}">
            <label for="${safeId}">
                <span style="display:inline-block;width:16px;height:16px;background:${color};border-radius:2px;margin-right:6px;"></span>
                ${rep}
            </label>
        `;

        cont.appendChild(row);

        document.getElementById(safeId).addEventListener("change", () => {
            aplicarCoberturaRep(rep);
        });
    });
}


// -------------------------
// RENDERIZAÇÃO DE FILTROS
// -------------------------
function renderRedes(redes){
  const container = document.getElementById('filtroRedeContainer');
  const legend = document.getElementById('redes-legend');
  container.innerHTML = '';
  legend.innerHTML = '';

  redes.forEach(rede => {
    const safeId = `rede-${rede.replace(/\s+/g,'-')}`;
    const color = REDE_COLORS[rede] || REDE_COLORS.DEFAULT;

    const row = document.createElement('div');
    row.className = 'checkbox-row';
    row.innerHTML = `
      <input type="checkbox" id="${safeId}" value="${rede}" checked>
      <label for="${safeId}">${rede}</label>
    `;
    container.appendChild(row);

    const item = document.createElement('div');
    item.className = 'legend-item';
    item.innerHTML = `
      <div class="legend-color-box" style="background:${color}"></div>
      <span>${rede}</span>
    `;
    legend.appendChild(item);
  });

  container.querySelectorAll('input[type=checkbox]')
    .forEach(ch => ch.addEventListener('change', onFiltroChange));
}

// -------------------------
// MARCADORES E POPUPS
// -------------------------
function makePopupHtml(u){
  return `
    <div class="popup-title">${u.nome || ''}</div>
    <div style="font-size:.9rem;margin-top:6px;">
      <strong>Rede:</strong> ${u.rede || 'N/A'}
    </div>
    <div style="font-size:.9rem;">
      <strong>Endereço:</strong> ${u.endereco_original || 'N/A'}
    </div>
    <div style="font-size:.85rem;color:var(--muted);margin-top:6px;">
      <strong>CNPJ:</strong> ${u.cnpj || 'N/A'}
    </div>
  `;
}

function criarMarcador(u){
  if(!u.latitude || !u.longitude) return null;

  const color = REDE_COLORS[u.rede] || REDE_COLORS.DEFAULT;
  const icon = L.divIcon({
    className: 'custom-marker',
    html: `<div style="width:100%;height:100%;border-radius:50%;background:${color}"></div>`,
    iconSize: [24,24],
    iconAnchor: [12,12]
  });

  const marker = L.marker([u.latitude, u.longitude], { icon });
  marker.bindPopup(makePopupHtml(u));
  return marker;
}

// -------------------------
// ATUALIZAÇÃO DO MAPA
// -------------------------
function atualizarMapa(unidadesList){
  unidadesAtuais = unidadesList;

  clusterGroup.clearLayers();
  markersLayer.clearLayers();

  try{ if(map.hasLayer(clusterGroup)) map.removeLayer(clusterGroup); }catch(e){}
  try{ if(map.hasLayer(markersLayer)) map.removeLayer(markersLayer); }catch(e){}

  unidadesList.forEach(u => {
    const m = criarMarcador(u);
    if(m) markersLayer.addLayer(m);
  });

  if(clustersAtivos){
    clusterGroup.addLayer(markersLayer);
    map.addLayer(clusterGroup);
  }else{
    map.addLayer(markersLayer);
  }

  if(markersLayer.getLayers().length > 0){
    try{
      map.fitBounds(markersLayer.getBounds(), { padding: [40,40] });
    }catch(e){}
  }
}

// -------------------------
// FILTRAGEM (sem cidades, sem heatmap)
// -------------------------
function lerFiltrosForm(){
  const redes = Array.from(
    document.querySelectorAll('#filtroRedeContainer input[type=checkbox]:checked')
  ).map(i => i.value);

  const estado = document.getElementById('selectEstado').value;
  const cnpj = document.getElementById('inputCNPJ').value.trim();
  const nome = document.getElementById('inputNome').value.trim();
  const ordenar = document.getElementById('selectOrdenar').value;

  return { redes, estado, cnpj, nome, ordenar };
}

async function applyFilters(){
  const status = document.getElementById('loading-status');
  status.textContent = 'Filtrando...';

  const f = lerFiltrosForm();

  let resultado = [...unidades];

  if(f.redes.length){
    resultado = resultado.filter(u => f.redes.includes(u.rede));
  }

  if(f.estado && f.estado !== 'Todos'){
    resultado = resultado.filter(u =>
      (u.estado && normalizeString(u.estado) === normalizeString(f.estado)) ||
      (u.endereco_original && normalizeString(u.endereco_original).includes(`-${normalizeString(f.estado)}`))
    );
  }

  if(f.cnpj){
    resultado = resultado.filter(u => u.cnpj && u.cnpj.includes(f.cnpj));
  }

  if(f.nome){
    resultado = resultado.filter(u => 
      u.nome && normalizeString(u.nome).includes(normalizeString(f.nome))
    );
  }

  switch(f.ordenar){
    case 'nome_az':
      resultado.sort((a,b)=> (a.nome||'').localeCompare(b.nome||''));
      break;
    case 'nome_za':
      resultado.sort((a,b)=> (b.nome||'').localeCompare(a.nome||'')); 
      break;
    case 'rede_az':
      resultado.sort((a,b)=> (a.rede||'').localeCompare(b.rede||'')); 
      break;
    case 'rede_za':
      resultado.sort((a,b)=> (b.rede||'').localeCompare(a.rede||'')); 
      break;
  }

  atualizarMapa(resultado);
  status.textContent = 'Dados atualizados.';
}

// -------------------------
// HANDLERS
// -------------------------
function onFiltroChange(){ applyFilters(); }

function bindControls(){
  const cbClusters = document.getElementById('toggleClusters');
  if(cbClusters) cbClusters.addEventListener('change', e => {
    clustersAtivos = e.target.checked;
    applyFilters();
  });

  ['selectEstado','inputCNPJ','inputNome','selectOrdenar'].forEach(id => {
    const el = document.getElementById(id);
    if(!el) return;
    el.addEventListener('input', onFiltroChange);
    el.addEventListener('change', onFiltroChange);
  });

  const btnClear = document.getElementById('btnClearFilters');
  if(btnClear){
    btnClear.addEventListener('click', () => {
      document.querySelectorAll('#filtroRedeContainer input[type=checkbox]')
        .forEach(i => i.checked = true);

      document.getElementById('selectEstado').value = 'Todos';
      document.getElementById('inputCNPJ').value = '';
      document.getElementById('inputNome').value = '';
      document.getElementById('selectOrdenar').value = 'nome_az';
      document.getElementById('toggleClusters').checked = true;

      clustersAtivos = true;

      applyFilters();
    });
  }
}

// -------------------------
// CARREGAMENTO INICIAL
// -------------------------
async function carregarDados(){
  const status = document.getElementById('loading-status');
  status.textContent = 'Carregando dados...';

  try{
    const r = await fetch(ENDPOINT_REDES);
    const redes = await r.json();
    renderRedes(redes);

    const uresp = await fetch(ENDPOINT_ALL);
    unidades = await uresp.json();

    applyFilters();

    status.textContent = 'Dados carregados.';
  }catch(err){
    console.error(err);
    status.textContent = 'Erro ao carregar dados.';
  }
}

// -------------------------
// GEOJSON
// -------------------------
async function carregarGeojsonEstados(){
  try{
    const r = await fetch(GEOJSON_PATH);
    const data = await r.json();
    L.geoJSON(data, {
      style: () => ({
        color:'#555',
        weight:1,
        fillOpacity:0.05
      }),
      onEachFeature: (f,l)=>{
        if(f.properties && f.properties.name)
          l.bindPopup(f.properties.name);
      }
    }).addTo(map);
  }catch(e){
    console.warn('GeoJSON não carregado:', e);
  }
}

// -------------------------
// INICIALIZA MAPA
// -------------------------
function initMap(){
  map = L.map('map').setView([-12.9777, -38.4764], 12);
  L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: '&copy; OpenStreetMap contributors'
  }).addTo(map);

  map.addLayer(markersLayer);

  carregarGeojsonEstados();
  bindControls();
  carregarDados();
}

window.addEventListener('load', () => {
  initMap();
});

// =========================
// FIM
// =========================
