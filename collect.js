// collect.js v5.4 — Coletor Bling com estoques + itens NFe
//
// Mudanças vs v5.3.1:
//   - Nova coleta: estoques (endpoint /estoques/saldos) - super rápido
//   - NFes novas agora salvam itens (quantidade por SKU)
//   - NFes antigas no cache NÃO são reprocessadas (evita 40min retrabalho)
//   - Novo arquivo: data/estoques.json
//   - Itens vão junto no cache em detalhes[id].itens

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
const CACHE_FILE = path.join(DATA_DIR, 'nfes_cache.json');
const CACHE_CONTAS_FILE = path.join(DATA_DIR, 'contas_cache.json');  // v5.6
const ESTOQUES_FILE = path.join(DATA_DIR, 'estoques.json');
const TOKEN_FILE = path.join(__dirname, '.bling_refresh_token');

const BLING_CLIENT_ID = process.env.BLING_CLIENT_ID;
const BLING_CLIENT_SECRET = process.env.BLING_CLIENT_SECRET;
const BLING_REFRESH_TOKEN = process.env.BLING_REFRESH_TOKEN;

const API_BASE = 'https://www.bling.com.br/Api/v3';
const RATE_LIMIT_MS = 400;
const DETAIL_RATE_LIMIT_MS = 350;
const MAX_PAGES_FULL = 500;
const INCREMENTAL_DAYS_BACK = 3;
const CHECKPOINT_INTERVAL = 500;

const NFE_SITUACOES_VALIDAS = [5, 6, 7];
const NFE_TIPO_SAIDA = 1;

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
// Cache de detalhes (com itens agora)
// ============================================================
function carregarCacheDetalhes() {
  if (fs.existsSync(CACHE_FILE)) {
    try {
      const cache = JSON.parse(fs.readFileSync(CACHE_FILE, 'utf-8'));
      const count = Object.keys(cache).length;
      const comItens = Object.values(cache).filter(function (d) { return d.itens && d.itens.length > 0; }).length;
      console.log('📥 Cache de detalhes: ' + count + ' NFes (' + comItens + ' com itens)');
      return cache;
    } catch (err) {
      console.log('⚠️  Cache corrompido: ' + err.message);
      return {};
    }
  }
  console.log('📥 Cache: nenhum encontrado');
  return {};
}

function salvarCacheDetalhes(cache) {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  fs.writeFileSync(CACHE_FILE, JSON.stringify(cache));
  const sizeKB = (fs.statSync(CACHE_FILE).size / 1024).toFixed(1);
  console.log('   💾 Checkpoint: ' + Object.keys(cache).length + ' detalhes (' + sizeKB + ' KB)');
}


// v5.6: Cache de detalhes de contas (categoria, histórico, centro custo)
function carregarCacheContas() {
  if (fs.existsSync(CACHE_CONTAS_FILE)) {
    try {
      const cache = JSON.parse(fs.readFileSync(CACHE_CONTAS_FILE, 'utf-8'));
      const count = Object.keys(cache).length;
      console.log('📥 Cache de contas: ' + count + ' contas detalhadas');
      return cache;
    } catch (err) {
      console.log('⚠️  Cache contas corrompido: ' + err.message);
      return {};
    }
  }
  console.log('📥 Cache de contas: nenhum encontrado');
  return {};
}

function salvarCacheContas(cache) {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  fs.writeFileSync(CACHE_CONTAS_FILE, JSON.stringify(cache));
  const sizeKB = (fs.statSync(CACHE_CONTAS_FILE).size / 1024).toFixed(1);
  console.log('   💾 Cache contas: ' + Object.keys(cache).length + ' (' + sizeKB + ' KB)');
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
// Extrator de itens (NOVO na v5.4)
// Recebe a resposta completa da NFe e extrai só o essencial dos itens
// ============================================================
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
      valorUnitario: parseFloat(item.valor) || 0,
      cfop: item.cfop || null  // v5.7: CFOP por item (pra filtrar venda vs remessa)
    };
  });
}

// ============================================================
// Enriquecimento COM ITENS
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

  console.log('\n🔍 Enriquecimento com itens (v5.4):');
  console.log('   NFes válidas: ' + validas.length);
  console.log('   Em cache: ' + Object.keys(jaTemosDetalhes).length);
  console.log('   A buscar: ' + precisaBuscar.length);

  if (precisaBuscar.length > 0) {
    const tempoEst = Math.ceil(precisaBuscar.length * DETAIL_RATE_LIMIT_MS / 60000);
    console.log('   Tempo estimado: ~' + tempoEst + ' min');
  }

  const detalhes = Object.assign({}, cacheDetalhes);
  let processados = 0;
  let novos = 0;
  let erros = 0;
  let desdeUltimoCheckpoint = 0;

  function buscarProximo(idx) {
    if (idx >= precisaBuscar.length) {
      if (desdeUltimoCheckpoint > 0) salvarCacheDetalhes(detalhes);
      return Promise.resolve(detalhes);
    }
    const nfe = precisaBuscar[idx];
    return apiGet(accessToken, '/nfe/' + nfe.id).then(function (resp) {
      if (resp && resp.data) {
        detalhes[nfe.id] = {
          valorNota: resp.data.valorNota,
          valorFrete: resp.data.valorFrete,
          serie: resp.data.serie,
          numeroPedidoLoja: resp.data.numeroPedidoLoja,
          chaveAcesso: resp.data.chaveAcesso,
          itens: extractItens(resp.data),  // v5.4
          formasPagamento: extractFormasPagamento(resp.data)  // v5.5
        };
        novos += 1;
      }
      processados += 1;
      desdeUltimoCheckpoint += 1;

      if (processados % 50 === 0) {
        const pct = (processados / precisaBuscar.length * 100).toFixed(1);
        console.log('   📦 ' + processados + '/' + precisaBuscar.length + ' (' + pct + '%) — novos: ' + novos + ' | erros: ' + erros);
      }

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
    console.log('✅ Detalhes coletados: ' + Object.keys(detalhes).length + ' (novos: ' + novos + ', erros: ' + erros + ')');
    return detalhes;
  });
}

// ============================================================
// v5.7: Helper pra detectar se NFe é venda (vs remessa/devolução/outros)
// ============================================================
// CFOPs de VENDA (válidos pra contabilizar faturamento):
// 5101, 5102, 5103, 5104, 5115, 5116, 5117, 5118, 5119, 5120, 5122, 5123
// 5401, 5402, 5403, 5405 (venda substituição tributária)
// 5501, 5502 (exportação - venda)
// 6101, 6102, 6103, 6104, 6107, 6108, 6115, 6116, 6117, 6118, 6119, 6120, 6122, 6123
// 6401, 6402, 6403, 6404, 6405 (venda ST interestadual)
// 6501, 6502, 7101, 7102 (exportação)
//
// CFOPs que NÃO são venda (devem ser EXCLUÍDOS):
// 5152, 5409 (transferência), 5201, 5202 (devolução), 5910, 5912, 5913 (remessa/bonificação/demo)
// 5915, 5916, 5917 (remessa pra conserto, demonstração), 5949 (outra saída não especificada)
// 6152, 6201, 6202, 6910, 6913, 6915, 6917 etc (mesmas operações interestaduais)
//
// finalidade 4 = Devolução (não é venda)
function isVenda(nfe) {
  // Entrada (E) nunca é venda, sempre descarta
  if (nfe.tipoOperacao === 'E' || nfe.tipo === 'E') return false;
  // Finalidade 4 = devolução. 2 = complementar (ok se da venda). 3 = ajuste.
  if (nfe.finalidade === 4 || nfe.finalidade === '4') return false;
  // Se não tem CFOP ainda coletado (NFe sem detalhe), dar benefício da dúvida
  if (!nfe.cfopPrimeiroItem && (!nfe.cfops || nfe.cfops.length === 0)) return true;
  // Testar CFOP. Venda: prefixo 5.1xx, 5.4xx, 5.5xx, 6.1xx, 6.4xx, 6.5xx, 7.1xx
  // Checa o primeiro item (geralmente o da operação principal)
  const cfop = String(nfe.cfopPrimeiroItem || (nfe.cfops && nfe.cfops[0]) || '');
  if (!cfop) return true;  // Sem CFOP: dar benefício da dúvida
  // Filtro positivo: CFOPs que são explicitamente de VENDA
  const vendaPrefixes = ['510', '540', '550', '610', '640', '650', '710'];
  const naoVendaPrefixes = ['515', '520', '591', '594', '615', '620', '691', '694'];
  const prefix3 = cfop.slice(0, 3);
  if (naoVendaPrefixes.indexOf(prefix3) >= 0) return false;
  if (vendaPrefixes.indexOf(prefix3) >= 0) return true;
  // Fallback: qualquer CFOP começando com 5 ou 6 que não foi classificado → dar benefício da dúvida
  // (a maioria das operações comerciais brasileiras)
  return cfop.charAt(0) === '5' || cfop.charAt(0) === '6' || cfop.charAt(0) === '7';
}

// ============================================================
// Consolidação NFes (agora inclui itens)
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
      base.itens = det.itens || [];                       // v5.4
      base.formasPagamento = det.formasPagamento || [];   // v5.5
      // v5.7: campos fiscais pra filtrar só vendas
      base.naturezaOperacao = det.naturezaOperacao || null;
      base.finalidade = det.finalidade || null;
      base.tipoOperacao = det.tipoOperacao || n.tipo || null;
      base.cfopPrimeiroItem = det.cfopPrimeiroItem || null;
      base.cfops = det.cfops || [];
      base.temDetalhe = true;
      base.ehVenda = isVenda(base);  // v5.7: true se é NFe de venda real
    } else {
      base.valorNota = 0;
      base.itens = [];
      base.formasPagamento = [];
      base.temDetalhe = false;
    }
    return base;
  });
}

// ============================================================
// Coleta estoques (NOVO na v5.4)
// Endpoint: /estoques/saldos
// ============================================================
function coletarEstoques(accessToken, produtos) {
  console.log('\n📦 Coletando estoques...');
  const productIds = produtos.map(function (p) { return p.id; }).filter(Boolean);
  if (productIds.length === 0) {
    console.log('   ⚠️  Sem produtos para buscar estoques');
    return Promise.resolve([]);
  }

  // API: podemos buscar múltiplos IDs de uma vez com ?idsProdutos[]
  // Mas como são só 18, fazemos um por um com throttle
  const resultados = [];
  let idx = 0;

  function proximo() {
    if (idx >= productIds.length) return Promise.resolve(resultados);
    const id = productIds[idx];
    idx += 1;
    return apiGet(accessToken, '/estoques/saldos?idsProdutos[]=' + id).then(function (resp) {
      if (resp && resp.data) {
        (Array.isArray(resp.data) ? resp.data : [resp.data]).forEach(function (s) {
          resultados.push({
            produtoId: id,
            depositoId: s.deposito && s.deposito.id,
            saldoFisico: parseFloat(s.saldoFisicoTotal) || 0,
            saldoVirtual: parseFloat(s.saldoVirtualTotal) || 0
          });
        });
      }
      if (idx % 5 === 0) console.log('   📊 Estoques: ' + idx + '/' + productIds.length);
      return sleep(300).then(proximo);
    }).catch(function (err) {
      console.log('   ⚠️  Erro estoque prod ' + id + ': ' + err.message);
      return sleep(300).then(proximo);
    });
  }

  return proximo().then(function (all) {
    console.log('✅ Estoques coletados: ' + all.length + ' registros');
    return all;
  });
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

  // v5.7: NFes válidas = situação ok + saída + com detalhe + é VENDA (não remessa/devolução)
  const nfesComDetalhe = nfes.filter(function (n) {
    return NFE_SITUACOES_VALIDAS.indexOf(Number(n.situacao)) !== -1 &&
           Number(n.tipo) === NFE_TIPO_SAIDA && n.temDetalhe;
  });
  const nfesValidas = nfesComDetalhe.filter(function (n) {
    return n.ehVenda !== false;  // true ou undefined (sem info) passa
  });
  const nfesExcluidas = nfesComDetalhe.length - nfesValidas.length;

  console.log('\n📊 NFes com detalhe: ' + nfesComDetalhe.length);
  console.log('📊 NFes VÁLIDAS (vendas): ' + nfesValidas.length);
  if (nfesExcluidas > 0) {
    console.log('🚫 Excluídas (remessa/devolução/outros): ' + nfesExcluidas);
    // Listar CFOPs mais comuns das excluídas pra auditoria
    const cfopsExcluidos = {};
    nfesComDetalhe.filter(function(n){ return n.ehVenda === false; })
      .forEach(function(n){
        const cfop = n.cfopPrimeiroItem || 'sem-cfop';
        cfopsExcluidos[cfop] = (cfopsExcluidos[cfop] || 0) + 1;
      });
    const topExcluidos = Object.entries(cfopsExcluidos).sort(function(a,b){return b[1]-a[1];}).slice(0, 5);
    console.log('   Top CFOPs excluídos: ' + topExcluidos.map(function(e){ return e[0]+' ('+e[1]+')'; }).join(', '));
  }

  const nfesB2B = nfesValidas.filter(function (n) { return n.tipoPessoa === 'J'; });
  const nfesDTC = nfesValidas.filter(function (n) { return n.tipoPessoa === 'F'; });

  console.log('   B2B: ' + nfesB2B.length + ' | DTC: ' + nfesDTC.length);

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

  // NOVO v5.4: vendas por SKU
  const porSku = {};
  nfesValidas.forEach(function (n) {
    if (!n.itens || n.itens.length === 0) return;
    const dataStr = (n.dataEmissao || '').slice(0, 10);
    const tipo = n.tipoPessoa;  // F = DTC, J = B2B
    n.itens.forEach(function (item) {
      const cod = item.codigo || 'SEM_CODIGO';
      if (!porSku[cod]) {
        porSku[cod] = {
          codigo: cod,
          descricao: item.descricao,
          totalQtd: 0,
          totalValor: 0,
          qtdDTC: 0,
          qtdB2B: 0,
          porData: {}
        };
      }
      const qtd = parseFloat(item.quantidade) || 0;
      const val = qtd * (parseFloat(item.valorUnitario) || 0);
      porSku[cod].totalQtd += qtd;
      porSku[cod].totalValor += val;
      if (tipo === 'F') porSku[cod].qtdDTC += qtd;
      else if (tipo === 'J') porSku[cod].qtdB2B += qtd;
      if (dataStr) {
        if (!porSku[cod].porData[dataStr]) porSku[cod].porData[dataStr] = { qtd: 0, qtdDTC: 0, qtdB2B: 0 };
        porSku[cod].porData[dataStr].qtd += qtd;
        if (tipo === 'F') porSku[cod].porData[dataStr].qtdDTC += qtd;
        else if (tipo === 'J') porSku[cod].porData[dataStr].qtdB2B += qtd;
      }
    });
  });

  const comItens = nfesValidas.filter(function (n) { return n.itens && n.itens.length > 0; }).length;
  console.log('   Com itens: ' + comItens + ' / ' + nfesValidas.length + ' NFes (' + (comItens / nfesValidas.length * 100).toFixed(1) + '%)');
  console.log('   SKUs únicos vendidos: ' + Object.keys(porSku).length);

  return {
    totalNFesValidas: nfesValidas.length,
    faturamentoTotal: totalFat,
    ticketMedio: nfesValidas.length ? totalFat / nfesValidas.length : 0,
    periodo7d: agregar(nfesValidas, d7),
    periodo30d: agregar(nfesValidas, d30),
    periodo90d: agregar(nfesValidas, d90),
    porData: porData, porMes: porMes, porAno: porAno, porCanal: porCanal,
    porSku: porSku,  // <-- NOVO v5.4
    nfesComItens: comItens,
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

function coletarCategoriasReceitasDespesas(accessToken) {
  // v5.6: Lista todas as categorias de receita/despesa cadastradas no Bling.
  // Cada categoria tem o grupo de DRE vinculado (ex: 'Receita Bruta', 'Despesas Operacionais').
  return coletarPaginado(accessToken, {
    endpoint: '/categorias/receitas-despesas',
    label: 'categorias receitas/despesas',
    maxPages: 5
  }).catch(function (err) {
    console.log('  ⚠️  Erro categorias: ' + err.message);
    return [];
  });
}

function coletarProdutos(accessToken) {
  return coletarPaginado(accessToken, { endpoint: '/produtos', label: 'produtos', maxPages: 10 });
}

function coletarContas(accessToken, tipo) {
  const endpoint = tipo === 'pagar' ? '/contas/pagar' : '/contas/receber';
  return coletarPaginado(accessToken, { endpoint: endpoint, label: 'contas a ' + tipo, maxPages: 10 })
    .catch(function (err) { console.log('  ⚠️  Erro contas a ' + tipo + ': ' + err.message); return []; });
}

function enriquecerContasComDetalhe(accessToken, contas, tipo, cacheDetalhes) {
  // v5.6: Busca detalhe de cada conta. Contém categoria (→ DRE), histórico, contato.
  // Rate limit: 350ms entre calls. Cache pra evitar recalls.
  const endpoint = tipo === 'pagar' ? '/contas/pagar' : '/contas/receber';
  const precisaBuscar = [];
  contas.forEach(function (conta) {
    if (cacheDetalhes[conta.id]) {
      // Mescla dados do cache na conta
      Object.assign(conta, cacheDetalhes[conta.id]);
    } else {
      precisaBuscar.push(conta);
    }
  });

  console.log('  🔎 Detalhando contas a ' + tipo + ': ' + precisaBuscar.length + ' novas (cache: ' + (contas.length - precisaBuscar.length) + ')');

  // Limite por run pra não estourar tempo — faz até 500 por run
  const MAX_POR_RUN = 500;
  const aFazer = precisaBuscar.slice(0, MAX_POR_RUN);
  if (precisaBuscar.length > MAX_POR_RUN) {
    console.log('  ⚠️  Limite de ' + MAX_POR_RUN + ' detalhes/run atingido. Restam ' + (precisaBuscar.length - MAX_POR_RUN) + ' pra próxima run.');
  }

  let sucessos = 0, erros = 0;
  const inicio = Date.now();

  return aFazer.reduce(function (promise, conta, idx) {
    return promise.then(function () {
      return new Promise(function (resolve) { setTimeout(resolve, 350); })
        .then(function () {
          return axios.get(BLING_API_BASE + endpoint + '/' + conta.id, {
            headers: { Authorization: 'Bearer ' + accessToken },
            timeout: 15000
          });
        })
        .then(function (resp) {
          if (resp && resp.data && resp.data.data) {
            const d = resp.data.data;
            const detalhe = {
              categoria: d.categoria || null,          // {id, descricao}
              historico: d.historico || '',             // descrição livre
              contato: d.contato || null,               // {id, nome}
              ocorrencia: d.ocorrencia || null,         // tipo de pagamento
              dataEmissao: d.dataEmissao || null,
              vencimento: d.vencimento || null,
              competencia: d.competencia || null        // data de competência (pra DRE)
            };
            cacheDetalhes[conta.id] = detalhe;
            Object.assign(conta, detalhe);
            sucessos += 1;
          }
          if ((idx + 1) % 50 === 0) {
            const elapsed = ((Date.now() - inicio) / 1000).toFixed(0);
            console.log('    ' + (idx + 1) + '/' + aFazer.length + ' contas a ' + tipo + ' (' + elapsed + 's)');
          }
        })
        .catch(function (err) {
          erros += 1;
          if (erros <= 5) console.log('    ⚠️  Erro conta ' + conta.id + ': ' + err.message);
        });
    });
  }, Promise.resolve()).then(function () {
    console.log('  ✅ Detalhes contas a ' + tipo + ': ' + sucessos + ' sucessos, ' + erros + ' erros');
    return contas;
  });
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
// MAIN
// ============================================================
function main() {
  console.log('🚀 Suplemind Bling Collector v5.6 (+ categorias + DRE + contas detalhadas)');
  console.log('   Modo: ' + MODE);
  console.log('   Timestamp: ' + new Date().toISOString());
  console.log('');

  if (!BLING_CLIENT_ID || !BLING_CLIENT_SECRET || !BLING_REFRESH_TOKEN) {
    throw new Error('Credenciais Bling ausentes');
  }

  let accessToken;
  let cacheDetalhes = {};
  let cacheContas = {};  // v5.6

  return refreshAccessToken().then(function (token) {
    accessToken = token;
    cacheDetalhes = carregarCacheDetalhes();
    cacheContas = carregarCacheContas();  // v5.6

    if (MODE === 'incremental') {
      if (!fs.existsSync(DATA_FILE)) {
        console.log('⚠️  bling.json não existe — forçando full');
        return coletarModoFull(accessToken, cacheDetalhes, cacheContas);
      }
      return coletarModoIncremental(accessToken, cacheDetalhes, cacheContas);
    }

    fazerBackup();
    return coletarModoFull(accessToken, cacheDetalhes, cacheContas);
  }).then(function (dados) {
    relatarSituacoes(dados.nfes);

    // NOVO v5.4: coletar estoques (rápido, ~30s)
    return coletarEstoques(accessToken, dados.produtos).then(function (estoques) {
      dados.estoques = estoques;
      return dados;
    });
  }).then(function (dados) {
    console.log('\n📊 Stats + conciliação...');
    const conciliacao = conciliar(dados.pedidos, dados.nfes);
    const stats = computeStats(dados.pedidos, dados.nfes, dados.produtos, dados.contasPagar, dados.contasReceber, conciliacao);

    const output = {
      meta: {
        version: '5.6',
        collectedAt: new Date().toISOString(),
        mode: MODE,
        nfeSituacoesValidas: NFE_SITUACOES_VALIDAS,
        counts: {
          pedidos: dados.pedidos.length,
          nfes: dados.nfes.length,
          nfesValidas: stats.totalNFesValidas,
          nfesComItens: stats.nfesComItens,
          produtos: dados.produtos.length,
          estoques: (dados.estoques || []).length,
          skusVendidos: Object.keys(stats.porSku).length,
          contasPagar: dados.contasPagar.length,
          contasReceber: dados.contasReceber.length
        }
      },
      stats: stats,
      conciliacao: conciliacao,
      pedidos: dados.pedidos,
      nfes: dados.nfes,
      produtos: dados.produtos,
      estoques: dados.estoques || [],
      contasPagar: dados.contasPagar,
      contasReceber: dados.contasReceber,
      categoriasRecDesp: dados.categoriasRecDesp || []  // v5.6
    };

    if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
    // v5.6: Salvar cache contas antes de escrever JSON final
    salvarCacheContas(cacheContas);
    salvarCacheDetalhes(cacheDetalhes);  // garante último estado

    fs.writeFileSync(DATA_FILE, JSON.stringify(output, null, 2));

    // Salva também estoques em arquivo separado (pra consulta rápida)
    fs.writeFileSync(ESTOQUES_FILE, JSON.stringify({
      collectedAt: output.meta.collectedAt,
      estoques: dados.estoques || []
    }, null, 2));

    const sizeKB = (fs.statSync(DATA_FILE).size / 1024).toFixed(1);
    console.log('');
    console.log('✅ Coleta v5.4 concluída!');
    console.log('   Pedidos: ' + dados.pedidos.length);
    console.log('   NFes: ' + dados.nfes.length + ' (' + stats.nfesComItens + ' com itens)');
    console.log('   Faturamento: R$ ' + stats.faturamentoTotal.toFixed(2));
    console.log('     B2B: R$ ' + stats.faturamentoB2B.toFixed(2));
    console.log('     DTC: R$ ' + stats.faturamentoDTC.toFixed(2));
    console.log('   Estoques: ' + (dados.estoques || []).length + ' registros');
    console.log('   SKUs únicos vendidos: ' + Object.keys(stats.porSku).length);
    console.log('   bling.json: ' + sizeKB + ' KB');
  }).catch(function (err) {
    console.error('❌ ERRO: ' + err.message);
    console.error(err.stack);
    process.exit(1);
  });
}

function coletarModoFull(accessToken, cacheDetalhes, cacheContas) {
  return Promise.all([
    coletarPedidos(accessToken, { maxPages: MAX_PAGES_FULL }),
    coletarNFesLista(accessToken, { dataInicial: '2024-11-01' }),
    coletarProdutos(accessToken),
    coletarContas(accessToken, 'pagar'),
    coletarContas(accessToken, 'receber'),
    coletarCategoriasReceitasDespesas(accessToken)  // v5.6
  ]).then(function (r) {
    return enriquecerNFesComDetalhes(accessToken, r[1], cacheDetalhes)
      .then(function (detalhesMap) {
        // v5.6: enriquecer contas com categoria, histórico, centro custo
        return enriquecerContasComDetalhe(accessToken, r[3], 'pagar', cacheContas)
          .then(function () {
            return enriquecerContasComDetalhe(accessToken, r[4], 'receber', cacheContas);
          })
          .then(function () {
            return {
              pedidos: r[0],
              nfes: consolidarNFes(r[1], detalhesMap),
              produtos: r[2],
              contasPagar: r[3],
              contasReceber: r[4],
              categoriasRecDesp: r[5]  // v5.6
            };
          });
      });
  });
}

function coletarModoIncremental(accessToken, cacheDetalhes, cacheContas) {
  const existentes = JSON.parse(fs.readFileSync(DATA_FILE, 'utf-8'));
  const pedidosExistentes = existentes.pedidos || [];
  const nfesExistentes = existentes.nfes || [];
  const categoriasExistentes = existentes.categoriasRecDesp || [];

  const corte = new Date(Date.now() - INCREMENTAL_DAYS_BACK * 86400000);
  const dataInicial = corte.toISOString().slice(0, 10);
  const dataFinal = new Date().toISOString().slice(0, 10);

  console.log('📥 Incremental desde ' + dataInicial);

  return Promise.all([
    coletarPedidos(accessToken, { dataInicial: dataInicial, maxPages: 20 }),
    coletarNFesLista(accessToken, {
      janelas: [{ inicial: dataInicial, final: dataFinal }],
      maxPages: 20
    }),
    coletarProdutos(accessToken),
    coletarContas(accessToken, 'pagar'),     // v5.6: contas sempre atualizam
    coletarContas(accessToken, 'receber'),   // v5.6
    coletarCategoriasReceitasDespesas(accessToken)  // v5.6
  ]).then(function (r) {
    const pedidosNovos = r[0];
    const nfesListaNovas = r[1];
    const produtosNovos = r[2];
    const contasPagarNovas = r[3];
    const contasReceberNovas = r[4];
    const categoriasNovas = r[5].length > 0 ? r[5] : categoriasExistentes;

    return enriquecerNFesComDetalhes(accessToken, nfesListaNovas, cacheDetalhes)
      .then(function (detalhesMap) {
        const nfesNovas = consolidarNFes(nfesListaNovas, detalhesMap);
        // v5.6: enriquecer contas (usa cache)
        return enriquecerContasComDetalhe(accessToken, contasPagarNovas, 'pagar', cacheContas)
          .then(function () {
            return enriquecerContasComDetalhe(accessToken, contasReceberNovas, 'receber', cacheContas);
          })
          .then(function () {
            return {
              pedidos: mergeById(pedidosExistentes, pedidosNovos, 'pedidos'),
              nfes: mergeById(nfesExistentes, nfesNovas, 'NFes'),
              produtos: produtosNovos,
              contasPagar: contasPagarNovas,
              contasReceber: contasReceberNovas,
              categoriasRecDesp: categoriasNovas
            };
          });
      });
  });
}

main();
