const https = require('https');
const fs = require('fs');
const path = require('path');

const CLIENT_ID     = process.env.BLING_CLIENT_ID;
const CLIENT_SECRET = process.env.BLING_CLIENT_SECRET;
const REFRESH_TOKEN = process.env.BLING_REFRESH_TOKEN;

function httpRequest(options, body) {
  return new Promise(function(resolve, reject) {
    var req = https.request(options, function(res) {
      var data = '';
      res.on('data', function(chunk) { data += chunk; });
      res.on('end', function() {
        try { resolve({ status: res.statusCode, data: JSON.parse(data) }); }
        catch(e) { resolve({ status: res.statusCode, data: data }); }
      });
    });
    req.on('error', reject);
    if (body) req.write(body);
    req.end();
  });
}

function sleep(ms) { return new Promise(function(r) { setTimeout(r, ms); }); }

async function getAccessToken() {
  var credentials = Buffer.from(CLIENT_ID + ':' + CLIENT_SECRET).toString('base64');
  var body = 'grant_type=refresh_token&refresh_token=' + REFRESH_TOKEN;
  var res = await httpRequest({
    hostname: 'www.bling.com.br',
    path: '/Api/v3/oauth/token',
    method: 'POST',
    headers: {
      'Authorization': 'Basic ' + credentials,
      'Content-Type': 'application/x-www-form-urlencoded',
      'Content-Length': Buffer.byteLength(body)
    }
  }, body);
  if (res.status !== 200 || !res.data.access_token) {
    throw new Error('Falha ao obter token (HTTP ' + res.status + '): ' + JSON.stringify(res.data));
  }
  console.log('Token OK');
  if (res.data.refresh_token) {
    fs.writeFileSync('.bling_refresh_token', res.data.refresh_token);
  }
  return res.data.access_token;
}

async function blingGet(token, endpoint) {
  await sleep(350);
  var res = await httpRequest({
    hostname: 'www.bling.com.br',
    path: '/Api/v3/' + endpoint,
    method: 'GET',
    headers: { 'Authorization': 'Bearer ' + token, 'Accept': 'application/json' }
  });
  return res.data;
}

async function fetchTodoHistorico(token) {
  var allVendas = [];
  var p = 1;
  // 500 páginas = 50.000 pedidos (~4 anos com crescimento acelerado)
  var MAX_PAGES = 500;

  console.log('Iniciando coleta completa do historico (sem corte de data)...');

  while (p <= MAX_PAGES) {
    var r = await blingGet(token, 'pedidos/vendas?limite=100&pagina=' + p);
    var dados = (r && r.data) ? r.data : [];

    if (dados.length === 0) {
      console.log('Pagina ' + p + ': sem dados. Fim do historico atingido.');
      break;
    }

    allVendas = allVendas.concat(dados);

    if (p % 10 === 0 || dados.length < 100) {
      var oldest = (dados[dados.length-1].data || dados[dados.length-1].dataVenda || '').substring(0,10);
      console.log('Pagina ' + p + ' | Total: ' + allVendas.length + ' pedidos | Mais antigo nesta pag: ' + oldest);
    }

    if (dados.length < 100) {
      console.log('Ultima pagina atingida (' + p + ', ' + dados.length + ' pedidos).');
      break;
    }

    p++;
  }

  if (p > MAX_PAGES) {
    console.log('AVISO: limite de seguranca (' + MAX_PAGES + ' pags) atingido. Aumentar MAX_PAGES se necessario.');
  }

  var newest = allVendas.length > 0 ? (allVendas[0].data || allVendas[0].dataVenda || '').substring(0,10) : '';
  var oldest = allVendas.length > 0 ? (allVendas[allVendas.length-1].data || allVendas[allVendas.length-1].dataVenda || '').substring(0,10) : '';
  console.log('Historico completo: ' + allVendas.length + ' pedidos | ' + oldest + ' ate ' + newest);
  return allVendas;
}

async function main() {
  console.log('Suplemind Intelligence v5 — Coleta Completa');
  console.log(new Date().toLocaleString('pt-BR', { timeZone: 'America/Sao_Paulo' }));
  console.log('');

  var token = await getAccessToken();

  var allVendas = await fetchTodoHistorico(token);

  console.log('Coletando produtos...');
  var produtos = await blingGet(token, 'produtos?limite=100&pagina=1');
  var produtosData = (produtos && produtos.data) ? produtos.data : [];
  console.log(produtosData.length + ' produtos');

  console.log('Coletando contas a pagar (todas as paginas)...');
  var cpAll = [];
  for (var cp = 1; cp <= 10; cp++) {
    var cpR = await blingGet(token, 'contas/pagar?limite=100&pagina=' + cp);
    var cpD = (cpR && cpR.data) ? cpR.data : [];
    cpAll = cpAll.concat(cpD);
    if (cpD.length < 100) break;
  }
  console.log(cpAll.length + ' contas a pagar');

  console.log('Coletando contas a receber (todas as paginas)...');
  var crAll = [];
  for (var crP = 1; crP <= 10; crP++) {
    var crR = await blingGet(token, 'contas/receber?limite=100&pagina=' + crP);
    var crD = (crR && crR.data) ? crR.data : [];
    crAll = crAll.concat(crD);
    if (crD.length < 100) break;
  }
  console.log(crAll.length + ' contas a receber');

  // Estatísticas sobre todo o histórico
  var porData = {};
  var porMes = {};
  var porAno = {};
  var porCanal = {};
  var faturamentoTotal = 0;

  allVendas.forEach(function(v) {
    var val = parseFloat(v.totalVenda || v.total) || 0;
    faturamentoTotal += val;

    var d = (v.data || v.dataVenda || '').substring(0, 10);
    if (d) {
      if (!porData[d]) porData[d] = { count: 0, valor: 0 };
      porData[d].count++;
      porData[d].valor += val;

      var mes = d.substring(0, 7);
      if (!porMes[mes]) porMes[mes] = { count: 0, valor: 0 };
      porMes[mes].count++;
      porMes[mes].valor += val;

      var ano = d.substring(0, 4);
      if (!porAno[ano]) porAno[ano] = { count: 0, valor: 0 };
      porAno[ano].count++;
      porAno[ano].valor += val;
    }

    var canal = v.loja && v.loja.id ? 'Loja ' + v.loja.id : 'Direto';
    porCanal[canal] = (porCanal[canal] || 0) + 1;
  });

  var comValor = allVendas.filter(function(v) { return (parseFloat(v.totalVenda || v.total) || 0) > 0; });
  var ticketMedio = comValor.length > 0 ? faturamentoTotal / comValor.length : 0;
  var totalPagar = cpAll.reduce(function(s, c) { return s + (parseFloat(c.valor) || 0); }, 0);
  var totalReceber = crAll.reduce(function(s, c) { return s + (parseFloat(c.valor) || 0); }, 0);

  var output = {
    meta: {
      lastUpdate: new Date().toISOString(),
      source: 'Bling API v3',
      collectorVersion: '5.0',
      totalPedidosColetados: allVendas.length,
      periodoColetado: {
        inicio: allVendas.length > 0 ? (allVendas[allVendas.length-1].data || allVendas[allVendas.length-1].dataVenda || '').substring(0,10) : '',
        fim:    allVendas.length > 0 ? (allVendas[0].data || allVendas[0].dataVenda || '').substring(0,10) : ''
      }
    },
    stats: {
      totalVendas: allVendas.length,
      faturamentoTotal: parseFloat(faturamentoTotal.toFixed(2)),
      ticketMedio: parseFloat(ticketMedio.toFixed(2)),
      totalAPagar: parseFloat(totalPagar.toFixed(2)),
      totalAReceber: parseFloat(totalReceber.toFixed(2)),
      porData: Object.entries(porData).sort(function(a, b) { return a[0].localeCompare(b[0]); }),
      porMes:  Object.entries(porMes).sort(function(a, b)  { return a[0].localeCompare(b[0]); }),
      porAno:  Object.entries(porAno).sort(function(a, b)  { return a[0].localeCompare(b[0]); }),
      porCanal: porCanal
    },
    vendas: allVendas.map(function(v) {
      return {
        id: v.id, numero: v.numero,
        data: v.data || v.dataVenda,
        contato: (v.contato && v.contato.nome) || (v.cliente && v.cliente.nome) || '',
        total: parseFloat(v.totalVenda || v.total) || 0,
        situacao: v.situacao && v.situacao.valor,
        lojaId: v.loja && v.loja.id
      };
    }),
    produtos: produtosData.map(function(p) {
      return { id: p.id, nome: p.nome, codigo: p.codigo, preco: parseFloat(p.preco) || 0 };
    }),
    contasPagar: cpAll.map(function(c) {
      return { id: c.id, descricao: c.descricao, vencimento: c.dataVencimento, valor: parseFloat(c.valor) || 0, situacao: c.situacao && c.situacao.valor };
    }),
    contasReceber: crAll.map(function(c) {
      return { id: c.id, descricao: c.descricao, vencimento: c.dataVencimento, valor: parseFloat(c.valor) || 0, situacao: c.situacao && c.situacao.valor };
    })
  };

  var dataDir = path.join(__dirname, 'data');
  if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir, { recursive: true });
  fs.writeFileSync(path.join(dataDir, 'bling.json'), JSON.stringify(output, null, 2));

  console.log('');
  console.log('=== COLETA CONCLUIDA ===');
  console.log('Pedidos: ' + allVendas.length + ' (' + output.meta.periodoColetado.inicio + ' ate ' + output.meta.periodoColetado.fim + ')');
  console.log('Faturamento total historico: R$ ' + output.stats.faturamentoTotal);
  console.log('Ticket medio: R$ ' + output.stats.ticketMedio);
  console.log('Meses com dados: ' + Object.keys(porMes).length + ' | Anos: ' + Object.keys(porAno).join(', '));
}

main().catch(function(err) {
  console.error('ERRO CRITICO:', err.message);
  console.error(err.stack);
  process.exit(1);
});
