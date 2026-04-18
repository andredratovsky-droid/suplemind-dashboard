// collect.js v5.3.1 — Coletor Bling com checkpoint incremental
//
// Mudanças vs v5.3:
//   - NFE_DETAILS_CACHE: arquivo dedicado em data/nfes_cache.json que persiste
//     detalhes de NFes individualmente. Sobrevive a falhas do workflow.
//   - Checkpoint a cada 500 detalhes: salva progresso em disco.
//   - Retomada automática: se cache existir, só busca o que falta.
//   - Progresso mais verboso (cada 50 ao invés de 100).
//   - Salva cache mesmo se main() falhar (via try/finally global).

const fs = require('fs');
const path = require('path');
const https = require('https');

// ============================================================
// CONFIG
// ============================================================
const MODE = (process.env.COLLECT_MODE || 'full').toLowerCase();
const DATA_DIR = path.join(__dirname, 'data');
const DATA_FILE = path.join(DATA_DIR, 'bling.json');
const BACKUP_FILE = path.join(DATA_DIR, 'bling_backup_v5.2.json');
const CACHE_FILE = path.join(DATA_DIR, 'nfes_cache.json');  // NOVO
const TOKEN_FILE = path.join(__dirname, '.bling_refresh_token');

const BLING_CLIENT_ID = process.env.BLING_CLIENT_ID;
const BLING_CLIENT_SECRET = process.env.BLING_CLIENT_SECRET;
const BLING_REFRESH_TOKEN = process.env.BLING_REFRESH_TOKEN;

const API_BASE = 'https://www.bling.com.br/Api/v3';
const RATE_LIMIT_MS = 400;
const DETAIL_RATE_LIMIT_MS = 350;
const MAX_PAGES_FULL = 500;
const INCREMENTAL_DAYS_BACK = 3;
const CHECKPOINT_INTERVAL = 500;  // Salvar cache a cada N detalhes

// ============================================================
// SITUAÇÕES DE NFe
// Do log anterior: 6.209 de 6.219 (99.8%) passaram em [5, 6, 7] → códigos OK
// ============================================================
const NFE_SITUACOES_VALIDAS = [5, 6, 7];
const NFE_TIPO_SAIDA = 1;
const PEDIDO_SITUACOES_PAGAS = [1, 9];

const NFE_SIT_LABELS = {
  1: 'Pendente', 2: 'Cancelada', 3: 'Denegada', 4: 'Aguardando Recibo',
  5: 'Emitida DANFE', 6: 'Autorizada', 7: 'Emitida DANFE (alt)',
  8: 'Registrada', 9: 'Enviada', 10: 'Denegada'
};

// ============================================================
// HTTP helpers
// ============================================================
function httpRequest(options, body) {
  return new Promise(function (resolve, reject) {
    const req = https.request(options, function (res) {
      let chunks = '';
      res.on('data', function (c) { chunks += c; });
      res.on('end', function () {
        resolve({ status: res.statusCode, body: chunks, headers: res.headers });
      });
    });
    req.on('error', reject);
    if (body) req.write(body);
    req.end();
  });
}

function sleep(ms) { return new Promise(function (r) { setTimeout(r, ms); }); }

function detectTipoPessoa(numeroDocumento) {
  if (!numeroDocumento) return null;
  const clean = String(numeroDocumento).replace(/\D/g, '');
  if (clean.length === 11) return 'F';
  if (clean.length === 14) return 'J';
  return null;
}

// ============================================================
// OAuth
// ============================================================
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
    if (!data.access_token) throw new Error('Sem access_token');
    if (data.refresh_token && data.refresh_token !== BLING_REFRESH_TOKEN) {
      fs.writeFileSync(TOKEN_FILE, data.refresh_token);
      console.log('💾 Novo refresh_token salvo');
    }
    console.log('✅ Access token renovado');
    return data.access_token;
  });
}

function apiGet(accessToken, endpoint) {
  const url = new URL(API_BASE + endpoint);
  return httpRequest({
    hostname: url.hostname,
    path: url.pathname + url.search,
    method: 'GET',
    headers: {
      'Authorization': 'Bearer ' + accessToken,
      'Accept': 'application/json'
    }
  }, null).then(function (resp) {
    if (resp.status === 429) {
      return sleep(2000).then(function () { return apiGet(accessToken, endpoint); });
    }
    if (resp.status === 404) return { data: null };
    if (resp.status !== 200) {
      throw new Error('API ' + endpoint + ' retornou ' + resp.status + ': ' + resp.body.slice(0, 200));
    }
    return JSON.parse(resp.body);
  });
}

// ============================================================
// CHECKPOINT: cache de detalhes NFe
// ============================================================
function carregarCacheDetalhes() {
  if (fs.existsSync(CACHE_FILE)) {
    try {
      const cache = JSON.parse(fs.readFileSync(CACHE_FILE, 'utf-8'));
      const count = Object.keys(cache).length;
      console.log('📥 Cache de detalhes carregado de ' + path.basename(CACHE_FILE) + ': ' + count + ' NFes');
      return cache;
    } catch (err) {
      console.log('⚠️  Cache corrompido, ignorando: ' + err.message);
      return {};
    }
  }
  console.log('📥 Cache de detalhes: nenhum encontrado (começando do zero)');
  return {};
}

function salvarCacheDetalhes(cache) {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  fs.writeFileSync(CACHE_FILE, JSON.stringify(cache));
  const sizeKB = (fs.statSync(CACHE_FILE).size / 1024).toFixed(1);
  console.log('   💾 Checkpoint salvo: ' + Object.keys(cache).length + ' detalhes (' + sizeKB + ' KB)');
}

// ============================================================
// Coleta paginada genérica
// ============================================================
function coletarPaginado(accessToken, config) {
  const todos = [];
  let pagina = 1;
  const label = config.label || 'items';

  console.log('📦 Coletando ' + label + '...');

  function nextPage() {
    if (pagina > (config.maxPages || MAX_PAGES_FULL)) {
      console.log('  ⚠️  Limite ' + config.maxPages + ' páginas em ' + label);
      return Promise.resolve(todos);
    }
    const query = config.buildQuery ? config.buildQuery(pagina) : ('?limite=100&pagina=' + pagina);
    return apiGet(accessToken, config.endpoint + query).then(function (resp) {
      const items = (resp && resp.data) || [];
      if (items.length === 0) {
        console.log('  ✅ Fim de ' + label + ' na pág ' + pagina + ' (total: ' + todos.length + ')');
        return todos;
      }
      if (config.stopAtId) {
        for (let i = 0; i < items.length; i++) {
          if (String(items[i].id) === String(config.stopAtId)) {
            todos.push.apply(todos, items.slice(0, i));
            console.log('  ✅ Parou em ID ' + config.stopAtId);
            return todos;
          }
        }
      }
      todos.push.apply(todos, items);
      if (pagina % 5 === 0 || pagina === 1) {
        console.log('  📄 ' + label + ' pág ' + pagina + ': +' + items.length + ' (total: ' + todos.length + ')');
      }
      pagina += 1;
      return sleep(RATE_LIMIT_MS).then(nextPage);
    });
  }
  return nextPage();
}

function coletarPedidos(accessToken, opts) {
  opts = opts || {};
  return coletarPaginado(accessToken, {
    endpoint: '/pedidos/vendas',
    label: 'pedidos',
    maxPages: opts.maxPages || MAX_PAGES_FULL,
    stopAtId: opts.stopAtId || null,
    buildQuery: function (pagina) {
      let q = '?limite=100&pagina=' + pagina;
      if (opts.dataInicial) q += '&dataEmissaoInicial=' + opts.dataInicial;
      return q;
    }
  });
}

function gerarJanelasAnuais(dataInicialStr) {
  const hoje = new Date();
  const inicial = new Date(dataInicialStr + 'T00:00:00Z');
  const janelas = [];
  let cursor = new Date(hoje.getTime());
  while (cursor > inicial) {
    const finalJ = cursor.toISOString().slice(0, 10);
    const inicialJ = new Date(cursor.getTime() - 364 * 86400000);
    const inicialJStr = inicialJ < inicial ? inicial.toISOString().slice(0, 10) : inicialJ.toISOString().slice(0, 10);
    janelas.push({ inicial: inicialJStr, final: finalJ });
    cursor = new Date(inicialJ.getTime() - 86400000);
  }
  return janelas;
}

function coletarNFesLista(accessToken, opts) {
  opts = opts || {};
  const janelas = opts.janelas || gerarJanelasAnuais(opts.dataInicial || '2024-11-01');
  console.log('📄 Coletando lista de NFes em ' + janelas.length + ' janela(s)...');
  const todas = [];
  let idx = 0;

  function proxima() {
    if (idx >= janelas.length) return Promise.resolve(todas);
    const j = janelas[idx];
    idx += 1;
    console.log('  🗓️  Janela ' + idx + '/' + janelas.length + ': ' + j.inicial + ' a ' + j.final);
    return coletarPaginado(accessToken, {
      endpoint: '/nfe',
      label: 'NFes (' + j.inicial.slice(0, 7) + ')',
      maxPages: opts.maxPages || 200,
      buildQuery: function (pagina) {
        return '?limite=100&pagina=' + pagina +
          '&dataEmissaoInicial=' + j.inicial +
          '&dataEmissaoFinal=' + j.final +
          '&tipo=' + NFE_TIPO_SAIDA;
      }
    }).then(function (nfes) {
      todas.push.apply(todas, nfes);
      return sleep(RATE_LIMIT_MS).then(proxima);
    });
  }
  return proxima().then(function (all) {
    console.log('✅ Total de NFes (listagem): ' + all.length);
    return all;
  });
}

// ============================================================
// Enriquecimento COM CHECKPOINT
// ============================================================
function enriquecerNFesComDetalhes(accessToken, nfes, cacheDetalhes) {
  const validas = nfes.filter(function (n) {
    return NFE_SITUACOES_VALIDAS.indexOf(Number(n.situacao)) !== -1 &&
           Number(n.tipo) === NFE_TIPO_SAIDA;
  });

  const precisaBuscar = [];
  const jaTemosDetalhes = {};

  validas.forEach(function (n) {
    if (cacheDetalhes[n.id]) {
      jaTemosDetalhes[n.id] = cacheDetalhes[n.id];
    } else {
      precisaBuscar.push(n);
    }
  });

  console.log('\n🔍 Enriquecimento de detalhes (com checkpoint):');
  console.log('   NFes válidas: ' + validas.length);
  console.log('   Em cache: ' + Object.keys(jaTemosDetalhes).length);
  console.log('   A buscar: ' + precisaBuscar.length);

  if (precisaBuscar.length > 0) {
    const tempoEst = Math.ceil(precisaBuscar.length * DETAIL_RATE_LIMIT_MS / 60000);
    console.log('   Tempo estimado: ~' + tempoEst + ' min');
    console.log('   Checkpoint a cada ' + CHECKPOINT_INTERVAL + ' NFes salvas em ' + path.basename(CACHE_FILE));
  }

  // Começa cópia do cache existente
  const detalhes = Object.assign({}, cacheDetalhes);
  let processados = 0;
  let novos = 0;
  let erros = 0;
  let desdeUltimoCheckpoint = 0;

  function buscarProximo(idx) {
    if (idx >= precisaBuscar.length) {
      // Salva cache final
      if (desdeUltimoCheckpoint > 0) salvarCacheDetalhes(detalhes);
      return Promise.resolve(detalhes);
    }
    const nfe = precisaBuscar[idx];
    return apiGet(accessToken, '/nfe/' + nfe.id).then(function (resp) {
      if (resp && resp.data) {
        // Salva só os campos que precisamos (economiza memória e disco)
        detalhes[nfe.id] = {
          valorNota: resp.data.valorNota,
          valorFrete: resp.data.valorFrete,
          serie: resp.data.serie,
          numeroPedidoLoja: resp.data.numeroPedidoLoja,
          chaveAcesso: resp.data.chaveAcesso
        };
        novos += 1;
      }
      processados += 1;
      desdeUltimoCheckpoint += 1;

      if (processados % 50 === 0) {
        const pct = (processados / precisaBuscar.length * 100).toFixed(1);
        console.log('   📦 ' + processados + '/' + precisaBuscar.length + ' (' + pct + '%) — novos: ' + novos + ' | erros: ' + erros);
      }

      // CHECKPOINT
      if (desdeUltimoCheckpoint >= CHECKPOINT_INTERVAL) {
        salvarCacheDetalhes(detalhes);
        desdeUltimoCheckpoint = 0;
      }

      return sleep(DETAIL_RATE_LIMIT_MS).then(function () {
        return buscarProximo(idx + 1);
      });
    }).catch(function (err) {
      erros += 1;
      if (erros <= 3) console.log('   ⚠️  Erro NFe ' + nfe.id + ': ' + err.message);
      processados += 1;
      desdeUltimoCheckpoint += 1;

      // Mesmo em erro, faz checkpoint periódico
      if (desdeUltimoCheckpoint >= CHECKPOINT_INTERVAL) {
        salvarCacheDetalhes(detalhes);
        desdeUltimoCheckpoint = 0;
      }

      return sleep(DETAIL_RATE_LIMIT_MS).then(function () {
        return buscarProximo(idx + 1);
      });
    });
  }

  return buscarProximo(0).then(function () {
    console.log('✅ Detalhes coletados: ' + Object.keys(detalhes).length + ' (novos nesta rodada: ' + novos + ', erros: ' + erros + ')');
    return detalhes;
  });
}

// ============================================================
// Consolidação: lista + cache → array final
// ============================================================
function consolidarNFes(nfesLista, detalhesMap) {
  return nfesLista.map(function (n) {
    const det = detalhesMap[n.id];
    const base = {
      id: n.id,
      tipo: n.tipo,
      situacao: n.situacao,
      numero: n.numero,
      dataEmissao: n.dataEmissao,
      contato: n.contato,
      loja: n.loja,
      tipoPessoa: detectTipoPessoa(n.contato && n.contato.numeroDocumento)
    };
    if (det) {
      base.valorNota = parseFloat(det.valorNota) || 0;
      base.valorFrete = parseFloat(det.valorFrete) || 0;
      base.serie = det.serie;
      base.numeroPedidoLoja = det.numeroPedidoLoja || null;
      base.chaveAcesso = det.chaveAcesso;
      base.temDetalhe = true;
    } else {
      base.valorNota = 0;
      base.temDetalhe = false;
    }
    return base;
  });
}

// ============================================================
// Coleta produtos, contas
// ============================================================
function coletarProdutos(accessToken) {
  return coletarPaginado(accessToken, { endpoint: '/produtos', label: 'produtos', maxPages: 10 });
}

function coletarContas(accessToken, tipo) {
  const endpoint = tipo === 'pagar' ? '/contas/pagar' : '/contas/receber';
  return coletarPaginado(accessToken, { endpoint: endpoint, label: 'contas a ' + tipo, maxPages: 10 })
    .catch(function (err) { console.log('  ⚠️  Erro contas a ' + tipo + ': ' + err.message); return []; });
}

function mergeById(existentes, novos, label) {
  const byId = {};
  for (let i = 0; i < existentes.length; i++) byId[existentes[i].id] = existentes[i];
  let atualizados = 0, adicionados = 0;
  for (let i = 0; i < novos.length; i++) {
    if (byId[novos[i].id]) atualizados += 1; else adicionados += 1;
    byId[novos[i].id] = novos[i];
  }
  const merged = Object.values(byId);
  merged.sort(function (a, b) {
    const da = new Date(a.data || a.dataEmissao || 0).getTime();
    const db = new Date(b.data || b.dataEmissao || 0).getTime();
    return db - da;
  });
  console.log('🔀 Merge ' + label + ': +' + adicionados + ' novos, ' + atualizados + ' atualizados, total ' + merged.length);
  return merged;
}

// ============================================================
// Conciliação
// ============================================================
function conciliar(pedidos, nfes) {
  const pedidosByNumero = {};
  const pedidosByNumeroLoja = {};
  pedidos.forEach(function (p) {
    if (p.numero) pedidosByNumero[String(p.numero)] = p;
    if (p.numeroLoja) pedidosByNumeroLoja[String(p.numeroLoja)] = p;
  });

  const nfePedidoLink = {};
  const pedidoNfeLink = {};
  const nfesSemPedidoIds = [];
  const valoresDivergentes = [];

  nfes.forEach(function (nfe) {
    if (!nfe.temDetalhe) return;
    let pedido = null;
    if (nfe.numeroPedidoLoja) {
      const np = String(nfe.numeroPedidoLoja);
      pedido = pedidosByNumeroLoja[np] || pedidosByNumero[np];
    }
    if (pedido) {
      nfePedidoLink[nfe.id] = pedido.id;
      pedidoNfeLink[pedido.id] = nfe.id;
      const valNfe = parseFloat(nfe.valorNota) || 0;
      const valPed = parseFloat(pedido.total) || 0;
      if (Math.abs(valNfe - valPed) > 0.01) {
        valoresDivergentes.push({
          pedidoId: pedido.id, pedidoNumero: pedido.numero,
          nfeId: nfe.id, nfeNumero: nfe.numero,
          valorPedido: valPed, valorNFe: valNfe, diff: valNfe - valPed
        });
      }
    } else {
      nfesSemPedidoIds.push(nfe.id);
    }
  });

  const pedidosSemNFeIds = pedidos.filter(function (p) { return !pedidoNfeLink[p.id]; }).map(function (p) { return p.id; });

  console.log('\n🔗 Conciliação pedido ↔ NFe:');
  console.log('   NFes com pedido vinculado: ' + Object.keys(nfePedidoLink).length);
  console.log('   NFes SEM pedido (B2B avulso?): ' + nfesSemPedidoIds.length);
  console.log('   Pedidos SEM NFe: ' + pedidosSemNFeIds.length);
  console.log('   Valores divergentes: ' + valoresDivergentes.length);

  return {
    nfePedidoLink: nfePedidoLink,
    pedidoNfeLink: pedidoNfeLink,
    nfesSemPedidoIds: nfesSemPedidoIds,
    pedidosSemNFeIds: pedidosSemNFeIds,
    valoresDivergentes: valoresDivergentes,
    resumo: {
      totalNFes: nfes.length, totalPedidos: pedidos.length,
      nfesComPedido: Object.keys(nfePedidoLink).length,
      nfesSemPedido: nfesSemPedidoIds.length,
      pedidosSemNFe: pedidosSemNFeIds.length,
      valoresDivergentes: valoresDivergentes.length
    }
  };
}

function relatarSituacoes(nfes) {
  const dist = {};
  nfes.forEach(function (n) {
    const sit = n.situacao;
    dist[sit] = (dist[sit] || 0) + 1;
  });
  console.log('\n📊 Distribuição de situações de NFe:');
  Object.keys(dist).sort().forEach(function (sit) {
    const label = NFE_SIT_LABELS[sit] || 'desconhecida';
    const flag = NFE_SITUACOES_VALIDAS.indexOf(Number(sit)) !== -1 ? '✅' : '⚪';
    console.log('   ' + flag + ' situacao=' + sit + ' (' + label + '): ' + dist[sit] + ' NFes');
  });
}

// ============================================================
// Stats
// ============================================================
function computeStats(pedidos, nfes, produtos, contasPagar, contasReceber, conciliacao) {
  const hoje = new Date();
  const d7 = new Date(hoje.getTime() - 7 * 86400000);
  const d30 = new Date(hoje.getTime() - 30 * 86400000);
  const d90 = new Date(hoje.getTime() - 90 * 86400000);

  const nfesValidas = nfes.filter(function (n) {
    return NFE_SITUACOES_VALIDAS.indexOf(Number(n.situacao)) !== -1 &&
           Number(n.tipo) === NFE_TIPO_SAIDA && n.temDetalhe;
  });

  console.log('\n📊 NFes válidas com detalhe completo: ' + nfesValidas.length);

  const nfesB2B = nfesValidas.filter(function (n) { return n.tipoPessoa === 'J'; });
  const nfesDTC = nfesValidas.filter(function (n) { return n.tipoPessoa === 'F'; });
  const nfesSemTipo = nfesValidas.filter(function (n) { return !n.tipoPessoa; });

  console.log('   B2B (CNPJ): ' + nfesB2B.length);
  console.log('   DTC (CPF):  ' + nfesDTC.length);
  console.log('   Sem tipo: ' + nfesSemTipo.length);

  function agregar(lista, desde) {
    const filtradas = desde ? lista.filter(function (n) {
      return new Date(n.dataEmissao || 0) >= desde;
    }) : lista;
    const fat = filtradas.reduce(function (s, n) { return s + (parseFloat(n.valorNota) || 0); }, 0);
    return { nfes: filtradas.length, faturamento: fat, ticketMedio: filtradas.length ? fat / filtradas.length : 0 };
  }

  const porData = {}, porMes = {}, porAno = {}, porCanal = {};

  nfesValidas.forEach(function (n) {
    const dataStr = (n.dataEmissao || '').slice(0, 10);
    if (!dataStr) return;
    const mesStr = dataStr.slice(0, 7);
    const anoStr = dataStr.slice(0, 4);
    const valor = parseFloat(n.valorNota) || 0;

    if (!porData[dataStr]) porData[dataStr] = { nfes: 0, faturamento: 0 };
    porData[dataStr].nfes += 1;
    porData[dataStr].faturamento += valor;

    if (!porMes[mesStr]) porMes[mesStr] = { nfes: 0, faturamento: 0 };
    porMes[mesStr].nfes += 1;
    porMes[mesStr].faturamento += valor;

    if (!porAno[anoStr]) porAno[anoStr] = { nfes: 0, faturamento: 0 };
    porAno[anoStr].nfes += 1;
    porAno[anoStr].faturamento += valor;

    let canalId = 'sem_canal';
    const pedidoId = conciliacao.nfePedidoLink[n.id];
    if (pedidoId) {
      const ped = pedidos.find(function (p) { return p.id === pedidoId; });
      if (ped && ped.loja && ped.loja.id) canalId = String(ped.loja.id);
    } else if (n.tipoPessoa === 'J') {
      canalId = 'b2b_avulso';
    } else if (n.loja && n.loja.id) {
      canalId = String(n.loja.id);
    }

    if (!porCanal[canalId]) porCanal[canalId] = { nfes: 0, faturamento: 0 };
    porCanal[canalId].nfes += 1;
    porCanal[canalId].faturamento += valor;
  });

  const totalFat = nfesValidas.reduce(function (s, n) { return s + (parseFloat(n.valorNota) || 0); }, 0);

  return {
    totalNFesValidas: nfesValidas.length,
    faturamentoTotal: totalFat,
    ticketMedio: nfesValidas.length ? totalFat / nfesValidas.length : 0,
    periodo7d: agregar(nfesValidas, d7),
    periodo30d: agregar(nfesValidas, d30),
    periodo90d: agregar(nfesValidas, d90),
    porData: porData, porMes: porMes, porAno: porAno, porCanal: porCanal,
    faturamentoB2B: nfesB2B.reduce(function (s, n) { return s + (parseFloat(n.valorNota) || 0); }, 0),
    faturamentoDTC: nfesDTC.reduce(function (s, n) { return s + (parseFloat(n.valorNota) || 0); }, 0),
    nfesB2B: nfesB2B.length,
    nfesDTC: nfesDTC.length,
    totalPedidos: pedidos.length,
    totalProdutos: produtos.length,
    totalContasPagar: contasPagar.length,
    somaContasPagar: contasPagar.reduce(function (s, c) { return s + (parseFloat(c.valor) || 0); }, 0),
    totalContasReceber: contasReceber.length,
    somaContasReceber: contasReceber.reduce(function (s, c) { return s + (parseFloat(c.valor) || 0); }, 0)
  };
}

function fazerBackup() {
  if (fs.existsSync(DATA_FILE)) {
    try {
      fs.copyFileSync(DATA_FILE, BACKUP_FILE);
      console.log('💾 Backup: ' + path.basename(BACKUP_FILE));
    } catch (err) {
      console.log('⚠️  Backup falhou: ' + err.message);
    }
  }
}

// ============================================================
// MAIN
// ============================================================
function main() {
  console.log('🚀 Suplemind Bling Collector v5.3.1 (checkpoint)');
  console.log('   Modo: ' + MODE);
  console.log('   Timestamp: ' + new Date().toISOString());
  console.log('   NFe situações válidas: [' + NFE_SITUACOES_VALIDAS.join(', ') + ']');
  console.log('   Checkpoint interval: ' + CHECKPOINT_INTERVAL + ' NFes');
  console.log('');

  if (!BLING_CLIENT_ID || !BLING_CLIENT_SECRET || !BLING_REFRESH_TOKEN) {
    throw new Error('Credenciais Bling ausentes');
  }

  let accessToken;
  let cacheDetalhes = {};

  return refreshAccessToken().then(function (token) {
    accessToken = token;
    cacheDetalhes = carregarCacheDetalhes();

    if (MODE === 'incremental') {
      if (!fs.existsSync(DATA_FILE)) {
        console.log('⚠️  bling.json não existe — forçando modo full');
        return coletarModoFull(accessToken, cacheDetalhes);
      }
      return coletarModoIncremental(accessToken, cacheDetalhes);
    }

    fazerBackup();
    return coletarModoFull(accessToken, cacheDetalhes);
  }).then(function (dados) {
    relatarSituacoes(dados.nfes);

    console.log('\n📊 Calculando conciliação e estatísticas...');
    const conciliacao = conciliar(dados.pedidos, dados.nfes);
    const stats = computeStats(dados.pedidos, dados.nfes, dados.produtos, dados.contasPagar, dados.contasReceber, conciliacao);

    const output = {
      meta: {
        version: '5.3.1',
        collectedAt: new Date().toISOString(),
        mode: MODE,
        nfeSituacoesValidas: NFE_SITUACOES_VALIDAS,
        counts: {
          pedidos: dados.pedidos.length,
          nfes: dados.nfes.length,
          nfesValidas: stats.totalNFesValidas,
          produtos: dados.produtos.length,
          contasPagar: dados.contasPagar.length,
          contasReceber: dados.contasReceber.length
        }
      },
      stats: stats,
      conciliacao: conciliacao,
      pedidos: dados.pedidos,
      nfes: dados.nfes,
      produtos: dados.produtos,
      contasPagar: dados.contasPagar,
      contasReceber: dados.contasReceber
    };

    if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.writeFileSync(DATA_FILE, JSON.stringify(output, null, 2));

    const sizeKB = (fs.statSync(DATA_FILE).size / 1024).toFixed(1);
    console.log('');
    console.log('✅ Coleta concluída!');
    console.log('   Pedidos: ' + dados.pedidos.length);
    console.log('   NFes: ' + dados.nfes.length + ' total, ' + stats.totalNFesValidas + ' válidas');
    console.log('   Faturamento total (NFes válidas): R$ ' + stats.faturamentoTotal.toFixed(2));
    console.log('     B2B: R$ ' + stats.faturamentoB2B.toFixed(2) + ' (' + stats.nfesB2B + ' NFes)');
    console.log('     DTC: R$ ' + stats.faturamentoDTC.toFixed(2) + ' (' + stats.nfesDTC + ' NFes)');
    console.log('   Arquivo: ' + sizeKB + ' KB');
  }).catch(function (err) {
    console.error('❌ ERRO: ' + err.message);
    console.error(err.stack);
    // Cache já foi salvo incrementalmente — próxima rodada continua de onde parou
    process.exit(1);
  });
}

function coletarModoFull(accessToken, cacheDetalhes) {
  return Promise.all([
    coletarPedidos(accessToken, { maxPages: MAX_PAGES_FULL }),
    coletarNFesLista(accessToken, { dataInicial: '2024-11-01' }),
    coletarProdutos(accessToken),
    coletarContas(accessToken, 'pagar'),
    coletarContas(accessToken, 'receber')
  ]).then(function (r) {
    return enriquecerNFesComDetalhes(accessToken, r[1], cacheDetalhes)
      .then(function (detalhesMap) {
        return {
          pedidos: r[0],
          nfes: consolidarNFes(r[1], detalhesMap),
          produtos: r[2],
          contasPagar: r[3],
          contasReceber: r[4]
        };
      });
  });
}

function coletarModoIncremental(accessToken, cacheDetalhes) {
  const existentes = JSON.parse(fs.readFileSync(DATA_FILE, 'utf-8'));
  const pedidosExistentes = existentes.pedidos || [];
  const nfesExistentes = existentes.nfes || [];

  const corte = new Date(Date.now() - INCREMENTAL_DAYS_BACK * 86400000);
  const dataInicial = corte.toISOString().slice(0, 10);
  const dataFinal = new Date().toISOString().slice(0, 10);

  console.log('📥 Incremental desde ' + dataInicial);

  return Promise.all([
    coletarPedidos(accessToken, { dataInicial: dataInicial, maxPages: 20 }),
    coletarNFesLista(accessToken, {
      janelas: [{ inicial: dataInicial, final: dataFinal }],
      maxPages: 20
    })
  ]).then(function (r) {
    const pedidosNovos = r[0];
    const nfesListaNovas = r[1];
    return enriquecerNFesComDetalhes(accessToken, nfesListaNovas, cacheDetalhes)
      .then(function (detalhesMap) {
        const nfesNovas = consolidarNFes(nfesListaNovas, detalhesMap);
        return {
          pedidos: mergeById(pedidosExistentes, pedidosNovos, 'pedidos'),
          nfes: mergeById(nfesExistentes, nfesNovas, 'NFes'),
          produtos: existentes.produtos || [],
          contasPagar: existentes.contasPagar || [],
          contasReceber: existentes.contasReceber || []
        };
      });
  });
}

main();
