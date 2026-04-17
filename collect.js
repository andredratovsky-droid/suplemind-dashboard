// collect.js v5.1 — Coletor Bling com suporte a modo incremental
//
// Modos (via env COLLECT_MODE):
//   - "full"        : coleta TODAS as páginas de pedidos (até 500), produtos, contas
//   - "incremental" : coleta apenas pedidos dos últimos 3 dias e faz merge com bling.json existente
//
// Sempre atualiza: refresh token (arquivo .bling_refresh_token), data/bling.json

const fs = require('fs');
const path = require('path');
const https = require('https');

// ============================================================
// CONFIG
// ============================================================
const MODE = (process.env.COLLECT_MODE || 'full').toLowerCase();
const DATA_DIR = path.join(__dirname, 'data');
const DATA_FILE = path.join(DATA_DIR, 'bling.json');
const TOKEN_FILE = path.join(__dirname, '.bling_refresh_token');

const BLING_CLIENT_ID = process.env.BLING_CLIENT_ID;
const BLING_CLIENT_SECRET = process.env.BLING_CLIENT_SECRET;
const BLING_REFRESH_TOKEN = process.env.BLING_REFRESH_TOKEN;

const API_BASE = 'https://www.bling.com.br/Api/v3';
const RATE_LIMIT_MS = 350;
const MAX_PAGES_FULL = 500;
const INCREMENTAL_DAYS_BACK = 3; // margem para capturar pedidos editados/atualizados

// ============================================================
// HTTP helper
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

function sleep(ms) {
  return new Promise(function (r) { setTimeout(r, ms); });
}

// ============================================================
// OAuth: refresh access token
// ============================================================
function refreshAccessToken() {
  console.log('🔑 Renovando access token...');
  const auth = Buffer.from(BLING_CLIENT_ID + ':' + BLING_CLIENT_SECRET).toString('base64');
  const body = 'grant_type=refresh_token&refresh_token=' + encodeURIComponent(BLING_REFRESH_TOKEN);

  const options = {
    hostname: 'www.bling.com.br',
    path: '/Api/v3/oauth/token',
    method: 'POST',
    headers: {
      'Authorization': 'Basic ' + auth,
      'Content-Type': 'application/x-www-form-urlencoded',
      'Content-Length': Buffer.byteLength(body),
      'Accept': '1.0'
    }
  };

  return httpRequest(options, body).then(function (resp) {
    if (resp.status !== 200) {
      throw new Error('OAuth falhou (' + resp.status + '): ' + resp.body);
    }
    const data = JSON.parse(resp.body);
    if (!data.access_token) throw new Error('Sem access_token na resposta: ' + resp.body);

    // Salvar novo refresh_token para o workflow atualizar o Secret
    if (data.refresh_token && data.refresh_token !== BLING_REFRESH_TOKEN) {
      fs.writeFileSync(TOKEN_FILE, data.refresh_token);
      console.log('💾 Novo refresh_token salvo em .bling_refresh_token');
    }

    console.log('✅ Access token renovado (expira em ' + data.expires_in + 's)');
    return data.access_token;
  });
}

// ============================================================
// API call helper
// ============================================================
function apiGet(accessToken, endpoint) {
  const url = new URL(API_BASE + endpoint);
  const options = {
    hostname: url.hostname,
    path: url.pathname + url.search,
    method: 'GET',
    headers: {
      'Authorization': 'Bearer ' + accessToken,
      'Accept': 'application/json'
    }
  };

  return httpRequest(options, null).then(function (resp) {
    if (resp.status === 429) {
      console.log('  ⏳ Rate limit, aguardando 2s...');
      return sleep(2000).then(function () { return apiGet(accessToken, endpoint); });
    }
    if (resp.status !== 200) {
      throw new Error('API ' + endpoint + ' retornou ' + resp.status + ': ' + resp.body.slice(0, 200));
    }
    return JSON.parse(resp.body);
  });
}

// ============================================================
// Coleta paginada de pedidos
// ============================================================
function coletarPedidos(accessToken, opts) {
  opts = opts || {};
  const maxPages = opts.maxPages || MAX_PAGES_FULL;
  const dataInicial = opts.dataInicial || null; // formato YYYY-MM-DD
  const stopAtId = opts.stopAtId || null; // se definido, para ao encontrar esse ID

  const todos = [];
  let pagina = 1;

  console.log('📦 Coletando pedidos (modo: ' + (dataInicial ? 'incremental desde ' + dataInicial : 'full') + ')');

  function nextPage() {
    if (pagina > maxPages) {
      console.log('⚠️  Limite de ' + maxPages + ' páginas atingido');
      return Promise.resolve(todos);
    }

    let endpoint = '/pedidos/vendas?limite=100&pagina=' + pagina;
    if (dataInicial) endpoint += '&dataEmissaoInicial=' + dataInicial;

    return apiGet(accessToken, endpoint).then(function (resp) {
      const pedidos = (resp && resp.data) || [];
      if (pedidos.length === 0) {
        console.log('  ✅ Fim dos dados na página ' + pagina);
        return todos;
      }

      // Checagem de stopAtId (otimização incremental)
      if (stopAtId) {
        for (let i = 0; i < pedidos.length; i++) {
          if (String(pedidos[i].id) === String(stopAtId)) {
            console.log('  ✅ Encontrou pedido já existente (ID ' + stopAtId + ') — parando');
            todos.push.apply(todos, pedidos.slice(0, i));
            return todos;
          }
        }
      }

      todos.push.apply(todos, pedidos);
      console.log('  📄 Página ' + pagina + ': ' + pedidos.length + ' pedidos (total: ' + todos.length + ')');
      pagina += 1;
      return sleep(RATE_LIMIT_MS).then(nextPage);
    });
  }

  return nextPage();
}

// ============================================================
// Coleta produtos
// ============================================================
function coletarProdutos(accessToken) {
  console.log('🛍️  Coletando produtos...');
  const todos = [];
  let pagina = 1;

  function nextPage() {
    if (pagina > 10) return Promise.resolve(todos);
    return apiGet(accessToken, '/produtos?limite=100&pagina=' + pagina).then(function (resp) {
      const produtos = (resp && resp.data) || [];
      if (produtos.length === 0) return todos;
      todos.push.apply(todos, produtos);
      pagina += 1;
      return sleep(RATE_LIMIT_MS).then(nextPage);
    });
  }

  return nextPage().then(function (p) {
    console.log('  ✅ ' + p.length + ' produtos');
    return p;
  });
}

// ============================================================
// Coleta contas a pagar / receber
// ============================================================
function coletarContas(accessToken, tipo) {
  console.log('💰 Coletando contas a ' + tipo + '...');
  const endpoint = tipo === 'pagar' ? '/contas/pagar' : '/contas/receber';
  const todos = [];
  let pagina = 1;

  function nextPage() {
    if (pagina > 10) return Promise.resolve(todos);
    return apiGet(accessToken, endpoint + '?limite=100&pagina=' + pagina).then(function (resp) {
      const contas = (resp && resp.data) || [];
      if (contas.length === 0) return todos;
      todos.push.apply(todos, contas);
      pagina += 1;
      return sleep(RATE_LIMIT_MS).then(nextPage);
    }).catch(function (err) {
      console.log('  ⚠️  Erro em contas a ' + tipo + ': ' + err.message);
      return todos;
    });
  }

  return nextPage().then(function (c) {
    console.log('  ✅ ' + c.length + ' contas a ' + tipo);
    return c;
  });
}

// ============================================================
// Merge incremental: combina pedidos novos com os existentes
// Deduplicação por ID, novos sobrescrevem (para capturar mudanças)
// ============================================================
function mergePedidos(existentes, novos) {
  const byId = {};
  for (let i = 0; i < existentes.length; i++) {
    byId[existentes[i].id] = existentes[i];
  }
  let atualizados = 0;
  let adicionados = 0;
  for (let i = 0; i < novos.length; i++) {
    if (byId[novos[i].id]) atualizados += 1;
    else adicionados += 1;
    byId[novos[i].id] = novos[i];
  }
  const merged = Object.values(byId);
  // Ordenar por data desc
  merged.sort(function (a, b) {
    const da = new Date(a.data || a.dataEmissao || 0).getTime();
    const db = new Date(b.data || b.dataEmissao || 0).getTime();
    return db - da;
  });
  console.log('🔀 Merge: ' + adicionados + ' adicionados, ' + atualizados + ' atualizados, ' + merged.length + ' total');
  return merged;
}

// ============================================================
// Estatísticas
// ============================================================
function computeStats(pedidos, produtos, contasPagar, contasReceber) {
  const hoje = new Date();
  const d7 = new Date(hoje.getTime() - 7 * 86400000);
  const d30 = new Date(hoje.getTime() - 30 * 86400000);
  const d90 = new Date(hoje.getTime() - 90 * 86400000);

  function periodo(desde) {
    const filtrados = pedidos.filter(function (p) {
      const d = new Date(p.data || p.dataEmissao || 0);
      return d >= desde && (p.situacao && (p.situacao.id === 1 || p.situacao.id === 9));
    });
    const fat = filtrados.reduce(function (s, p) { return s + (parseFloat(p.total) || 0); }, 0);
    return { pedidos: filtrados.length, faturamento: fat, ticketMedio: filtrados.length ? fat / filtrados.length : 0 };
  }

  // Por data (para gráficos)
  const porData = {};
  const porMes = {};
  const porAno = {};
  const porCanal = {};

  for (let i = 0; i < pedidos.length; i++) {
    const p = pedidos[i];
    const dataStr = (p.data || p.dataEmissao || '').slice(0, 10);
    if (!dataStr) continue;
    const mesStr = dataStr.slice(0, 7);
    const anoStr = dataStr.slice(0, 4);
    const total = parseFloat(p.total) || 0;
    const lojaId = (p.loja && p.loja.id) || 'sem_loja';

    porData[dataStr] = porData[dataStr] || { pedidos: 0, faturamento: 0 };
    porData[dataStr].pedidos += 1;
    porData[dataStr].faturamento += total;

    porMes[mesStr] = porMes[mesStr] || { pedidos: 0, faturamento: 0 };
    porMes[mesStr].pedidos += 1;
    porMes[mesStr].faturamento += total;

    porAno[anoStr] = porAno[anoStr] || { pedidos: 0, faturamento: 0 };
    porAno[anoStr].pedidos += 1;
    porAno[anoStr].faturamento += total;

    porCanal[lojaId] = porCanal[lojaId] || { pedidos: 0, faturamento: 0 };
    porCanal[lojaId].pedidos += 1;
    porCanal[lojaId].faturamento += total;
  }

  const totalFat = pedidos.reduce(function (s, p) { return s + (parseFloat(p.total) || 0); }, 0);

  return {
    totalPedidos: pedidos.length,
    faturamentoTotal: totalFat,
    ticketMedio: pedidos.length ? totalFat / pedidos.length : 0,
    periodo7d: periodo(d7),
    periodo30d: periodo(d30),
    periodo90d: periodo(d90),
    porData: porData,
    porMes: porMes,
    porAno: porAno,
    porCanal: porCanal,
    totalProdutos: produtos.length,
    totalContasPagar: contasPagar.length,
    somaContasPagar: contasPagar.reduce(function (s, c) { return s + (parseFloat(c.valor) || 0); }, 0),
    totalContasReceber: contasReceber.length,
    somaContasReceber: contasReceber.reduce(function (s, c) { return s + (parseFloat(c.valor) || 0); }, 0)
  };
}

// ============================================================
// MAIN
// ============================================================
function main() {
  console.log('🚀 Suplemind Bling Collector v5.1');
  console.log('   Modo: ' + MODE);
  console.log('   Timestamp: ' + new Date().toISOString());
  console.log('');

  if (!BLING_CLIENT_ID || !BLING_CLIENT_SECRET || !BLING_REFRESH_TOKEN) {
    throw new Error('Credenciais Bling ausentes. Configure os Secrets do GitHub.');
  }

  let accessToken;

  return refreshAccessToken().then(function (token) {
    accessToken = token;

    if (MODE === 'incremental') {
      // Carregar dados existentes
      if (!fs.existsSync(DATA_FILE)) {
        console.log('⚠️  bling.json não existe — forçando modo full');
        return coletarModoFull(accessToken);
      }

      const existentes = JSON.parse(fs.readFileSync(DATA_FILE, 'utf-8'));
      const pedidosExistentes = existentes.pedidos || [];

      // Data de corte: hoje - 3 dias
      const corte = new Date(Date.now() - INCREMENTAL_DAYS_BACK * 86400000);
      const dataInicial = corte.toISOString().slice(0, 10);

      return coletarPedidos(accessToken, {
        dataInicial: dataInicial,
        maxPages: 20, // suficiente para ~2.000 pedidos em 3 dias
        stopAtId: null // não usar stopAtId no incremental — queremos capturar updates
      }).then(function (pedidosNovos) {
        const pedidosMerged = mergePedidos(pedidosExistentes, pedidosNovos);
        return {
          pedidos: pedidosMerged,
          produtos: existentes.produtos || [],
          contasPagar: existentes.contasPagar || [],
          contasReceber: existentes.contasReceber || []
        };
      });
    }

    return coletarModoFull(accessToken);
  }).then(function (dados) {
    console.log('\n📊 Calculando estatísticas...');
    const stats = computeStats(dados.pedidos, dados.produtos, dados.contasPagar, dados.contasReceber);

    const output = {
      meta: {
        version: '5.1',
        collectedAt: new Date().toISOString(),
        mode: MODE,
        counts: {
          pedidos: dados.pedidos.length,
          produtos: dados.produtos.length,
          contasPagar: dados.contasPagar.length,
          contasReceber: dados.contasReceber.length
        }
      },
      stats: stats,
      pedidos: dados.pedidos,
      produtos: dados.produtos,
      contasPagar: dados.contasPagar,
      contasReceber: dados.contasReceber
    };

    if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.writeFileSync(DATA_FILE, JSON.stringify(output, null, 2));

    const sizeKB = (fs.statSync(DATA_FILE).size / 1024).toFixed(1);
    console.log('');
    console.log('✅ Coleta concluída com sucesso!');
    console.log('   Pedidos: ' + dados.pedidos.length);
    console.log('   Faturamento total: R$ ' + stats.faturamentoTotal.toFixed(2));
    console.log('   Arquivo: ' + DATA_FILE + ' (' + sizeKB + ' KB)');
  }).catch(function (err) {
    console.error('❌ ERRO: ' + err.message);
    console.error(err.stack);
    process.exit(1);
  });
}

function coletarModoFull(accessToken) {
  return Promise.all([
    coletarPedidos(accessToken, { maxPages: MAX_PAGES_FULL }),
    coletarProdutos(accessToken),
    coletarContas(accessToken, 'pagar'),
    coletarContas(accessToken, 'receber')
  ]).then(function (resultados) {
    return {
      pedidos: resultados[0],
      produtos: resultados[1],
      contasPagar: resultados[2],
      contasReceber: resultados[3]
    };
  });
}

main();
