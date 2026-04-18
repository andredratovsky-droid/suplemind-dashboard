// collect.js v5.2 — Coletor Bling com coleta dupla: pedidos + NFes
//
// Mudanças vs v5.1:
//   - Coleta adicional de /nfe (Notas Fiscais Eletrônicas)
//   - NFes são fonte-da-verdade para faturamento (B2B sem pedido aparecerá corretamente)
//   - Conciliação pedido ↔ NFe calculada automaticamente
//   - Backup automático do bling.json antigo antes de substituir
//   - Stats recalculadas com base em NFes autorizadas
//   - Janela histórica multi-ano para NFes (Bling limita 1 ano por request)
//
// Modos (via env COLLECT_MODE):
//   - "full"        : coleta histórica completa (pedidos + NFes multi-ano)
//   - "incremental" : coleta últimos 3 dias e faz merge

const fs = require('fs');
const path = require('path');
const https = require('https');

// ============================================================
// CONFIG
// ============================================================
const MODE = (process.env.COLLECT_MODE || 'full').toLowerCase();
const DATA_DIR = path.join(__dirname, 'data');
const DATA_FILE = path.join(DATA_DIR, 'bling.json');
const BACKUP_FILE = path.join(DATA_DIR, 'bling_backup_v5.1.json');
const TOKEN_FILE = path.join(__dirname, '.bling_refresh_token');

const BLING_CLIENT_ID = process.env.BLING_CLIENT_ID;
const BLING_CLIENT_SECRET = process.env.BLING_CLIENT_SECRET;
const BLING_REFRESH_TOKEN = process.env.BLING_REFRESH_TOKEN;

const API_BASE = 'https://www.bling.com.br/Api/v3';
const RATE_LIMIT_MS = 400;  // Um pouco mais conservador com 2 endpoints
const MAX_PAGES_FULL = 500;
const INCREMENTAL_DAYS_BACK = 3;

// ============================================================
// PARAMETRIZAÇÃO DE SITUAÇÕES DE NFe
// IMPORTANTE: Ajustar aqui se os códigos do Bling v3 forem diferentes
// Baseado na documentação Bling:
//   1 = Pendente    2 = Cancelada    3 = Denegada    4 = Aguardando Recibo
//   5 = Rejeitada   6 = Autorizada   7 = Emitida DANFE  8 = Registrada
// Segundo André: "Emitida" e "Emitida DANFE" = faturamento válido
// Interpretação: Autorizada (6) e Emitida DANFE (7) são os estados válidos
// ============================================================
const NFE_SITUACOES_VALIDAS = [6, 7];  // Autorizada + Emitida DANFE
const NFE_TIPO_SAIDA = 1;  // 0=Entrada, 1=Saída — só contamos saídas

// Pedido situações (mantém do v5.1)
const PEDIDO_SITUACOES_PAGAS = [1, 9];  // Pago + Entregue

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
// Coleta paginada genérica (usada por pedidos, NFes, etc)
// ============================================================
function coletarPaginado(accessToken, config) {
  // config: { endpoint, label, maxPages, buildQuery, transform, stopAtId }
  const todos = [];
  let pagina = 1;
  const label = config.label || 'items';

  console.log('📦 Coletando ' + label + '...');

  function nextPage() {
    if (pagina > (config.maxPages || MAX_PAGES_FULL)) {
      console.log('  ⚠️  Limite de ' + config.maxPages + ' páginas atingido em ' + label);
      return Promise.resolve(todos);
    }

    const query = config.buildQuery ? config.buildQuery(pagina) : ('?limite=100&pagina=' + pagina);
    const endpoint = config.endpoint + query;

    return apiGet(accessToken, endpoint).then(function (resp) {
      const items = (resp && resp.data) || [];
      if (items.length === 0) {
        console.log('  ✅ Fim de ' + label + ' na página ' + pagina + ' (total: ' + todos.length + ')');
        return todos;
      }

      if (config.stopAtId) {
        for (let i = 0; i < items.length; i++) {
          if (String(items[i].id) === String(config.stopAtId)) {
            console.log('  ✅ Encontrou ID ' + config.stopAtId + ' em ' + label + ' — parando');
            const transformados = config.transform ? items.slice(0, i).map(config.transform) : items.slice(0, i);
            todos.push.apply(todos, transformados);
            return todos;
          }
        }
      }

      const transformados = config.transform ? items.map(config.transform) : items;
      todos.push.apply(todos, transformados);
      if (pagina % 5 === 0 || pagina === 1) {
        console.log('  📄 ' + label + ' pág ' + pagina + ': +' + items.length + ' (total: ' + todos.length + ')');
      }
      pagina += 1;
      return sleep(RATE_LIMIT_MS).then(nextPage);
    });
  }

  return nextPage();
}

// ============================================================
// Coleta de pedidos
// ============================================================
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

// ============================================================
// Coleta de NFes — com múltiplas janelas de 1 ano (Bling limita)
// ============================================================
function coletarNFesMultiJanela(accessToken, opts) {
  opts = opts || {};

  // Janelas: [{inicial, final}, ...] em ordem cronológica decrescente
  const janelas = opts.janelas || gerarJanelasAnuais(opts.dataInicial || '2024-11-01');

  console.log('📄 Coletando NFes em ' + janelas.length + ' janela(s)...');

  const todas = [];
  let idx = 0;

  function proximaJanela() {
    if (idx >= janelas.length) return Promise.resolve(todas);
    const j = janelas[idx];
    idx += 1;
    console.log('  🗓️  Janela ' + idx + '/' + janelas.length + ': ' + j.inicial + ' a ' + j.final);

    return coletarPaginado(accessToken, {
      endpoint: '/nfe',
      label: 'NFes (' + j.inicial.slice(0, 7) + ')',
      maxPages: opts.maxPages || 200,
      buildQuery: function (pagina) {
        let q = '?limite=100&pagina=' + pagina;
        q += '&dataEmissaoInicial=' + j.inicial;
        q += '&dataEmissaoFinal=' + j.final;
        // tipo=1 filtra só NFes de saída — economiza requests
        q += '&tipo=' + NFE_TIPO_SAIDA;
        return q;
      }
    }).then(function (nfes) {
      todas.push.apply(todas, nfes);
      return sleep(RATE_LIMIT_MS).then(proximaJanela);
    });
  }

  return proximaJanela().then(function (todasNfes) {
    console.log('✅ Total de NFes coletadas: ' + todasNfes.length);
    return todasNfes;
  });
}

// Gera janelas de 1 ano (Bling tem limite de 1 ano por request)
function gerarJanelasAnuais(dataInicialStr) {
  const hoje = new Date();
  const hojeStr = hoje.toISOString().slice(0, 10);
  const inicial = new Date(dataInicialStr + 'T00:00:00Z');
  const janelas = [];

  let cursor = new Date(hoje.getTime());
  while (cursor > inicial) {
    const finalJ = cursor.toISOString().slice(0, 10);
    // Volta 364 dias (para ficar abaixo de 1 ano inteiro)
    const inicialJ = new Date(cursor.getTime() - 364 * 86400000);
    const inicialJStr = inicialJ < inicial ? inicial.toISOString().slice(0, 10) : inicialJ.toISOString().slice(0, 10);
    janelas.push({ inicial: inicialJStr, final: finalJ });
    cursor = new Date(inicialJ.getTime() - 86400000);
  }
  return janelas;
}

// ============================================================
// Coleta produtos, contas
// ============================================================
function coletarProdutos(accessToken) {
  return coletarPaginado(accessToken, {
    endpoint: '/produtos',
    label: 'produtos',
    maxPages: 10
  });
}

function coletarContas(accessToken, tipo) {
  const endpoint = tipo === 'pagar' ? '/contas/pagar' : '/contas/receber';
  return coletarPaginado(accessToken, {
    endpoint: endpoint,
    label: 'contas a ' + tipo,
    maxPages: 10
  }).catch(function (err) {
    console.log('  ⚠️  Erro em contas a ' + tipo + ': ' + err.message);
    return [];
  });
}

// ============================================================
// Merge incremental (pedidos e NFes)
// ============================================================
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
// Conciliação pedido ↔ NFe
// ============================================================
function conciliar(pedidos, nfes) {
  // Tentativa de vincular NFe a pedido via numero ou idOrigem
  const pedidosById = {};
  const pedidosByNumero = {};
  pedidos.forEach(function (p) {
    if (p.id) pedidosById[p.id] = p;
    if (p.numero) pedidosByNumero[p.numero] = p;
  });

  const nfePedidoLink = {};  // nfeId → pedidoId (se vinculado)
  const pedidoNfeLink = {};  // pedidoId → nfeId

  const nfesSemPedido = [];
  const nfesComPedido = [];
  const valoresDivergentes = [];

  nfes.forEach(function (nfe) {
    let pedidoVinculado = null;

    // Tentativa 1: idOrigem aponta para pedido
    if (nfe.idOrigem && pedidosById[nfe.idOrigem]) {
      pedidoVinculado = pedidosById[nfe.idOrigem];
    }
    // Tentativa 2: pedido com mesmo número
    if (!pedidoVinculado && nfe.numeroPedidoLoja) {
      pedidoVinculado = pedidosByNumero[nfe.numeroPedidoLoja];
    }
    // Tentativa 3: numeroLoja bate com numero do pedido
    if (!pedidoVinculado && nfe.numeroLoja) {
      pedidoVinculado = pedidosByNumero[nfe.numeroLoja];
    }

    if (pedidoVinculado) {
      nfePedidoLink[nfe.id] = pedidoVinculado.id;
      pedidoNfeLink[pedidoVinculado.id] = nfe.id;
      nfesComPedido.push(nfe);

      // Verificar divergência de valor
      const valNfe = parseFloat(nfe.valorNota) || 0;
      const valPed = parseFloat(pedidoVinculado.total) || 0;
      if (Math.abs(valNfe - valPed) > 0.01) {
        valoresDivergentes.push({
          pedidoId: pedidoVinculado.id,
          pedidoNumero: pedidoVinculado.numero,
          nfeId: nfe.id,
          valorPedido: valPed,
          valorNFe: valNfe,
          diff: valNfe - valPed
        });
      }
    } else {
      nfesSemPedido.push(nfe);
    }
  });

  const pedidosSemNFe = pedidos.filter(function (p) { return !pedidoNfeLink[p.id]; });

  console.log('');
  console.log('🔗 Conciliação pedido ↔ NFe:');
  console.log('   NFes com pedido vinculado: ' + nfesComPedido.length);
  console.log('   NFes SEM pedido (B2B avulso?): ' + nfesSemPedido.length);
  console.log('   Pedidos SEM NFe: ' + pedidosSemNFe.length);
  console.log('   Valores divergentes: ' + valoresDivergentes.length);

  return {
    nfePedidoLink: nfePedidoLink,
    pedidoNfeLink: pedidoNfeLink,
    nfesSemPedidoIds: nfesSemPedido.map(function (n) { return n.id; }),
    pedidosSemNFeIds: pedidosSemNFe.map(function (p) { return p.id; }),
    valoresDivergentes: valoresDivergentes,
    resumo: {
      totalNFes: nfes.length,
      totalPedidos: pedidos.length,
      nfesComPedido: nfesComPedido.length,
      nfesSemPedido: nfesSemPedido.length,
      pedidosSemNFe: pedidosSemNFe.length,
      valoresDivergentes: valoresDivergentes.length
    }
  };
}

// ============================================================
// Estatísticas (agora com base em NFes para faturamento)
// ============================================================
function computeStats(pedidos, nfes, produtos, contasPagar, contasReceber, conciliacao) {
  const hoje = new Date();
  const d7 = new Date(hoje.getTime() - 7 * 86400000);
  const d30 = new Date(hoje.getTime() - 30 * 86400000);
  const d90 = new Date(hoje.getTime() - 90 * 86400000);

  // NFes válidas (autorizadas/emitidas DANFE, tipo saída)
  const nfesValidas = nfes.filter(function (n) {
    const sit = n.situacao && (n.situacao.id !== undefined ? n.situacao.id : n.situacao);
    const tipo = n.tipo !== undefined ? n.tipo : NFE_TIPO_SAIDA;
    return NFE_SITUACOES_VALIDAS.indexOf(Number(sit)) !== -1 && Number(tipo) === NFE_TIPO_SAIDA;
  });

  console.log('📊 NFes válidas para faturamento: ' + nfesValidas.length + ' de ' + nfes.length);

  // Classificar NFes: B2B (tipoPessoa=J) vs DTC (tipoPessoa=F)
  const nfesB2B = nfesValidas.filter(function (n) {
    return n.contato && n.contato.tipoPessoa === 'J';
  });
  const nfesDTC = nfesValidas.filter(function (n) {
    return n.contato && n.contato.tipoPessoa === 'F';
  });
  const nfesSemTipo = nfesValidas.filter(function (n) {
    return !n.contato || !n.contato.tipoPessoa;
  });

  console.log('   B2B (CNPJ): ' + nfesB2B.length);
  console.log('   DTC (CPF):  ' + nfesDTC.length);
  console.log('   Sem classificação: ' + nfesSemTipo.length);

  // Função auxiliar: agrega NFes num período
  function agregarNFes(nfesLista, desde) {
    const filtradas = desde ? nfesLista.filter(function (n) {
      const d = new Date(n.dataEmissao || 0);
      return d >= desde;
    }) : nfesLista;
    const fat = filtradas.reduce(function (s, n) { return s + (parseFloat(n.valorNota) || 0); }, 0);
    return { nfes: filtradas.length, faturamento: fat, ticketMedio: filtradas.length ? fat / filtradas.length : 0 };
  }

  // Por data, mês, ano (baseado em NFes)
  const porData = {};
  const porMes = {};
  const porAno = {};
  const porCanal = {};  // canal vem do pedido vinculado, se houver

  nfesValidas.forEach(function (n) {
    const dataStr = (n.dataEmissao || '').slice(0, 10);
    if (!dataStr) return;
    const mesStr = dataStr.slice(0, 7);
    const anoStr = dataStr.slice(0, 4);
    const valor = parseFloat(n.valorNota) || 0;

    porData[dataStr] = porData[dataStr] || { nfes: 0, faturamento: 0 };
    porData[dataStr].nfes += 1;
    porData[dataStr].faturamento += valor;

    porMes[mesStr] = porMes[mesStr] || { nfes: 0, faturamento: 0 };
    porMes[mesStr].nfes += 1;
    porMes[mesStr].faturamento += valor;

    porAno[anoStr] = porAno[anoStr] || { nfes: 0, faturamento: 0 };
    porAno[anoStr].nfes += 1;
    porAno[anoStr].faturamento += valor;

    // Canal: via pedido vinculado OU fallback para B2B se for CNPJ sem pedido
    let canalId = 'sem_canal';
    const pedidoId = conciliacao.nfePedidoLink[n.id];
    if (pedidoId) {
      const ped = pedidos.find(function (p) { return p.id === pedidoId; });
      if (ped && ped.loja && ped.loja.id) canalId = String(ped.loja.id);
    } else if (n.contato && n.contato.tipoPessoa === 'J') {
      canalId = 'b2b_avulso';  // NFe B2B sem pedido — canal especial
    }

    porCanal[canalId] = porCanal[canalId] || { nfes: 0, faturamento: 0 };
    porCanal[canalId].nfes += 1;
    porCanal[canalId].faturamento += valor;
  });

  const totalFat = nfesValidas.reduce(function (s, n) { return s + (parseFloat(n.valorNota) || 0); }, 0);

  return {
    // Faturamento baseado em NFes (fonte-da-verdade)
    totalNFesValidas: nfesValidas.length,
    faturamentoTotal: totalFat,
    ticketMedio: nfesValidas.length ? totalFat / nfesValidas.length : 0,

    periodo7d: agregarNFes(nfesValidas, d7),
    periodo30d: agregarNFes(nfesValidas, d30),
    periodo90d: agregarNFes(nfesValidas, d90),

    porData: porData,
    porMes: porMes,
    porAno: porAno,
    porCanal: porCanal,

    // Classificação
    faturamentoB2B: nfesB2B.reduce(function (s, n) { return s + (parseFloat(n.valorNota) || 0); }, 0),
    faturamentoDTC: nfesDTC.reduce(function (s, n) { return s + (parseFloat(n.valorNota) || 0); }, 0),
    pedidosB2B: nfesB2B.length,
    pedidosDTC: nfesDTC.length,

    // Mantém referência aos pedidos (para clientes, recorrência, etc)
    totalPedidos: pedidos.length,

    // Outros
    totalProdutos: produtos.length,
    totalContasPagar: contasPagar.length,
    somaContasPagar: contasPagar.reduce(function (s, c) { return s + (parseFloat(c.valor) || 0); }, 0),
    totalContasReceber: contasReceber.length,
    somaContasReceber: contasReceber.reduce(function (s, c) { return s + (parseFloat(c.valor) || 0); }, 0)
  };
}

// ============================================================
// Backup do arquivo antigo
// ============================================================
function fazerBackup() {
  if (fs.existsSync(DATA_FILE)) {
    try {
      fs.copyFileSync(DATA_FILE, BACKUP_FILE);
      console.log('💾 Backup salvo em ' + path.basename(BACKUP_FILE));
    } catch (err) {
      console.log('⚠️  Não foi possível fazer backup: ' + err.message);
    }
  }
}

// ============================================================
// MAIN
// ============================================================
function main() {
  console.log('🚀 Suplemind Bling Collector v5.2 (pedidos + NFes)');
  console.log('   Modo: ' + MODE);
  console.log('   Timestamp: ' + new Date().toISOString());
  console.log('   NFe situações válidas: ' + NFE_SITUACOES_VALIDAS.join(', '));
  console.log('');

  if (!BLING_CLIENT_ID || !BLING_CLIENT_SECRET || !BLING_REFRESH_TOKEN) {
    throw new Error('Credenciais Bling ausentes. Configure os Secrets do GitHub.');
  }

  let accessToken;

  return refreshAccessToken().then(function (token) {
    accessToken = token;

    if (MODE === 'incremental') {
      if (!fs.existsSync(DATA_FILE)) {
        console.log('⚠️  bling.json não existe — forçando modo full');
        return coletarModoFull(accessToken);
      }
      return coletarModoIncremental(accessToken);
    }

    // Full: backup + coleta completa
    fazerBackup();
    return coletarModoFull(accessToken);
  }).then(function (dados) {
    console.log('\n📊 Calculando conciliação e estatísticas...');
    const conciliacao = conciliar(dados.pedidos, dados.nfes);
    const stats = computeStats(dados.pedidos, dados.nfes, dados.produtos, dados.contasPagar, dados.contasReceber, conciliacao);

    const output = {
      meta: {
        version: '5.2',
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
    console.log('✅ Coleta concluída com sucesso!');
    console.log('   Pedidos: ' + dados.pedidos.length);
    console.log('   NFes: ' + dados.nfes.length + ' (' + stats.totalNFesValidas + ' válidas)');
    console.log('   Faturamento total (NFes válidas): R$ ' + stats.faturamentoTotal.toFixed(2));
    console.log('     B2B: R$ ' + stats.faturamentoB2B.toFixed(2) + ' (' + stats.pedidosB2B + ' NFes)');
    console.log('     DTC: R$ ' + stats.faturamentoDTC.toFixed(2) + ' (' + stats.pedidosDTC + ' NFes)');
    console.log('   Arquivo: ' + DATA_FILE + ' (' + sizeKB + ' KB)');
  }).catch(function (err) {
    console.error('❌ ERRO: ' + err.message);
    console.error(err.stack);
    process.exit(1);
  });
}

function coletarModoFull(accessToken) {
  // Para NFes, usar histórico desde nov/2024 (data da primeira venda)
  return Promise.all([
    coletarPedidos(accessToken, { maxPages: MAX_PAGES_FULL }),
    coletarNFesMultiJanela(accessToken, { dataInicial: '2024-11-01' }),
    coletarProdutos(accessToken),
    coletarContas(accessToken, 'pagar'),
    coletarContas(accessToken, 'receber')
  ]).then(function (r) {
    return { pedidos: r[0], nfes: r[1], produtos: r[2], contasPagar: r[3], contasReceber: r[4] };
  });
}

function coletarModoIncremental(accessToken) {
  const existentes = JSON.parse(fs.readFileSync(DATA_FILE, 'utf-8'));
  const pedidosExistentes = existentes.pedidos || [];
  const nfesExistentes = existentes.nfes || [];

  const corte = new Date(Date.now() - INCREMENTAL_DAYS_BACK * 86400000);
  const dataInicial = corte.toISOString().slice(0, 10);
  const dataFinal = new Date().toISOString().slice(0, 10);

  console.log('📥 Modo incremental desde ' + dataInicial);

  return Promise.all([
    coletarPedidos(accessToken, { dataInicial: dataInicial, maxPages: 20 }),
    coletarNFesMultiJanela(accessToken, {
      janelas: [{ inicial: dataInicial, final: dataFinal }],
      maxPages: 20
    })
  ]).then(function (r) {
    const pedidosNovos = r[0];
    const nfesNovas = r[1];
    return {
      pedidos: mergeById(pedidosExistentes, pedidosNovos, 'pedidos'),
      nfes: mergeById(nfesExistentes, nfesNovas, 'NFes'),
      produtos: existentes.produtos || [],
      contasPagar: existentes.contasPagar || [],
      contasReceber: existentes.contasReceber || []
    };
  });
}

main();
