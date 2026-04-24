// enrich-items.js — Enriquece NFes do cache que ainda não têm itens
//
// Como funciona:
//   1. Lê data/nfes_cache.json
//   2. Identifica NFes que NÃO têm `itens` (array vazio ou ausente)
//   3. Rebusca /nfe/{id} e adiciona itens ao cache
//   4. Salva checkpoint a cada 500
//   5. Respeita timeout via env (MAX_MINUTES=15 por default)
//
// Uso: node enrich-items.js
// Env: BLING_CLIENT_ID, BLING_CLIENT_SECRET, BLING_REFRESH_TOKEN, MAX_MINUTES

const fs = require('fs');
const path = require('path');
const https = require('https');

const DATA_DIR = path.join(__dirname, 'data');
const CACHE_FILE = path.join(DATA_DIR, 'nfes_cache.json');
const TOKEN_FILE = path.join(__dirname, '.bling_refresh_token');

const BLING_CLIENT_ID = process.env.BLING_CLIENT_ID;
const BLING_CLIENT_SECRET = process.env.BLING_CLIENT_SECRET;
const BLING_REFRESH_TOKEN = process.env.BLING_REFRESH_TOKEN;
const MAX_MINUTES = parseInt(process.env.MAX_MINUTES || '15', 10);

const API_BASE = 'https://www.bling.com.br/Api/v3';
const RATE_LIMIT_MS = 350;
const CHECKPOINT_INTERVAL = 500;

const startTime = Date.now();

function httpRequest(options, body) {
  return new Promise(function (resolve, reject) {
    const req = https.request(options, function (res) {
      let chunks = '';
      res.on('data', function (c) { chunks += c; });
      res.on('end', function () { resolve({ status: res.statusCode, body: chunks }); });
    });
    req.on('error', reject);
    if (body) req.write(body);
    req.end();
  });
}

function sleep(ms) { return new Promise(function (r) { setTimeout(r, ms); }); }

function refreshAccessToken() {
  console.log('🔑 Renovando access token...');
  const auth = Buffer.from(BLING_CLIENT_ID + ':' + BLING_CLIENT_SECRET).toString('base64');
  const body = 'grant_type=refresh_token&refresh_token=' + encodeURIComponent(BLING_REFRESH_TOKEN);
  return httpRequest({
    hostname: 'www.bling.com.br',
    path: '/Api/v3/oauth/token',
    method: 'POST',
    headers: {
      'Authorization': 'Basic ' + auth,
      'Content-Type': 'application/x-www-form-urlencoded',
      'Content-Length': Buffer.byteLength(body),
      'Accept': '1.0'
    }
  }, body).then(function (resp) {
    if (resp.status !== 200) throw new Error('OAuth ' + resp.status + ': ' + resp.body);
    const data = JSON.parse(resp.body);
    if (data.refresh_token && data.refresh_token !== BLING_REFRESH_TOKEN) {
      fs.writeFileSync(TOKEN_FILE, data.refresh_token);
      console.log('💾 Novo refresh_token salvo');
    }
    return data.access_token;
  });
}

function apiGet(accessToken, endpoint) {
  const url = new URL(API_BASE + endpoint);
  return httpRequest({
    hostname: url.hostname,
    path: url.pathname + url.search,
    method: 'GET',
    headers: { 'Authorization': 'Bearer ' + accessToken, 'Accept': 'application/json' }
  }, null).then(function (resp) {
    if (resp.status === 429) return sleep(2000).then(function () { return apiGet(accessToken, endpoint); });
    if (resp.status !== 200) throw new Error('HTTP ' + resp.status + ': ' + resp.body.slice(0, 150));
    return JSON.parse(resp.body);
  });
}

function extractFormasPagamento(nfeData) {
  if (!nfeData) return [];
  var formas = [];
  if (nfeData.parcelas && Array.isArray(nfeData.parcelas)) {
    nfeData.parcelas.forEach(function(p) {
      if (p.formaPagamento) {
        formas.push({
          descricao: p.formaPagamento.descricao || p.formaPagamento.nome || 'Desconhecida',
          codigo: p.formaPagamento.codigoFiscal || p.formaPagamento.id || null,
          valor: parseFloat(p.valor) || 0
        });
      }
    });
  }
  if (formas.length === 0 && nfeData.formaPagamento) {
    var fp = nfeData.formaPagamento;
    formas.push({
      descricao: fp.descricao || fp.nome || 'Desconhecida',
      codigo: fp.codigoFiscal || fp.id || null,
      valor: parseFloat(nfeData.valorNota) || 0
    });
  }
  return formas;
}

function extractItens(nfeData) {
  if (!nfeData || !nfeData.itens || !Array.isArray(nfeData.itens)) return [];
  return nfeData.itens.map(function (item) {
    return {
      codigo: item.codigo || '',
      descricao: (item.descricao || '').slice(0, 100),
      quantidade: parseFloat(item.quantidade) || 0,
      valorUnitario: parseFloat(item.valor) || 0
    };
  });
}

function salvarCache(cache) {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  fs.writeFileSync(CACHE_FILE, JSON.stringify(cache));
  const sizeKB = (fs.statSync(CACHE_FILE).size / 1024).toFixed(1);
  console.log('💾 Cache salvo: ' + Object.keys(cache).length + ' NFes (' + sizeKB + ' KB)');
}

async function main() {
  console.log('🚀 Enrich Items v1.2 (itens + formas pgto + reprocessa antigas)');
  console.log('   Timeout: ' + MAX_MINUTES + ' min');
  console.log('');

  if (!fs.existsSync(CACHE_FILE)) {
    throw new Error('Cache não existe em ' + CACHE_FILE);
  }

  const cache = JSON.parse(fs.readFileSync(CACHE_FILE, 'utf-8'));
  const allIds = Object.keys(cache);
  const semItens = allIds.filter(function (id) {
    return !cache[id].itens || cache[id].itens.length === 0;
  });
  // v1.2: Também reprocessar NFes sem formasPagamento (adicionado no v1.1)
  const semFormaPag = allIds.filter(function (id) {
    // Tem itens mas não tem formasPagamento (foi enriquecido antes da v1.1)
    return cache[id].itens && cache[id].itens.length > 0
      && (!cache[id].formasPagamento);  // undefined significa ainda não coletado
  });

  // Mescla os dois sets (sem duplicar)
  const idsSet = new Set(semItens);
  semFormaPag.forEach(function(id){ idsSet.add(id); });
  const precisaProcessar = Array.from(idsSet);

  console.log('📊 Status do cache:');
  console.log('   Total: ' + allIds.length + ' NFes');
  console.log('   Com itens: ' + (allIds.length - semItens.length));
  console.log('   SEM itens: ' + semItens.length);
  console.log('   COM itens mas SEM formaPagamento (v1.2 reprocessa): ' + semFormaPag.length);
  console.log('   Total a processar: ' + precisaProcessar.length);

  if (precisaProcessar.length === 0) {
    console.log('✅ Todas NFes já têm itens E forma de pagamento! Nada a fazer.');
    return;
  }

  const tempoEst = Math.ceil(precisaProcessar.length * RATE_LIMIT_MS / 60000);
  console.log('   Tempo total estimado: ~' + tempoEst + ' min');
  console.log('   Nesta run (' + MAX_MINUTES + ' min): ~' + Math.floor(MAX_MINUTES * 60000 / RATE_LIMIT_MS) + ' NFes\n');

  const accessToken = await refreshAccessToken();

  let processados = 0;
  let sucessos = 0;
  let erros = 0;
  let desdeCheckpoint = 0;

  for (let i = 0; i < precisaProcessar.length; i++) {
    // Check timeout
    const elapsed = (Date.now() - startTime) / 60000;
    if (elapsed >= MAX_MINUTES) {
      console.log('\n⏱️  Timeout atingido (' + elapsed.toFixed(1) + ' min)');
      console.log('   Processados nesta run: ' + processados);
      console.log('   Sucesso: ' + sucessos + ' | Erros: ' + erros);
      console.log('   Restantes para próxima run: ' + (semItens.length - processados));
      break;
    }

    const id = precisaProcessar[i];
    try {
      const resp = await apiGet(accessToken, '/nfe/' + id);
      if (resp && resp.data) {
        const itens = extractItens(resp.data);
        const formas = extractFormasPagamento(resp.data);
        cache[id].itens = itens;
        cache[id].formasPagamento = formas;  // v1.1: forma pgto
        if (itens.length > 0 || formas.length > 0) sucessos += 1;
      }
    } catch (err) {
      erros += 1;
      if (erros <= 3) console.log('  ⚠️  Erro ' + id + ': ' + err.message);
    }

    processados += 1;
    desdeCheckpoint += 1;

    if (processados % 50 === 0) {
      const pct = (processados / semItens.length * 100).toFixed(1);
      const elMin = (elapsed).toFixed(1);
      console.log('   📦 ' + processados + '/' + semItens.length + ' (' + pct + '%) · ' + elMin + 'min · sucessos: ' + sucessos);
    }

    if (desdeCheckpoint >= CHECKPOINT_INTERVAL) {
      salvarCache(cache);
      desdeCheckpoint = 0;
    }

    await sleep(RATE_LIMIT_MS);
  }

  // Salva cache final
  salvarCache(cache);

  console.log('\n✅ Enriquecimento concluído (parcial ou total):');
  console.log('   Processados: ' + processados);
  console.log('   Sucesso: ' + sucessos);
  console.log('   Erros: ' + erros);
  console.log('   Restantes no cache: ' + (semItens.length - processados));
}

main().catch(function (err) {
  console.error('❌ ERRO: ' + err.message);
  console.error(err.stack);
  process.exit(1);
});
