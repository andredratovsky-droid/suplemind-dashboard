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

async function fetchAllVendas(token) {
  var allVendas = [];
  var p = 1;
  var MAX_PAGES = 50;
  var hoje = new Date();
  var corte = new Date(hoje);
  corte.setDate(corte.getDate() - 91);
  var cutoffStr = corte.toISOString().substring(0, 10);

  console.log('Buscando pedidos (limite: ' + MAX_PAGES + ' paginas | corte: ' + cutoffStr + ')...');

  while (p <= MAX_PAGES) {
    var r = await blingGet(token, 'pedidos/vendas?limite=100&pagina=' + p);
    var dados = (r && r.data) ? r.data : [];

    if (dados.length === 0) {
      console.log('Pagina ' + p + ': sem dados. Encerrando.');
      break;
    }

    allVendas = allVendas.concat(dados);
    console.log('Pagina ' + p + ': ' + dados.length + ' pedidos (total: ' + allVendas.length + ')');

    var ultimoData = (dados[dados.length - 1].data || dados[dados.length - 1].dataVenda || '').substring(0, 10);
    if (ultimoData && ultimoData < cutoffStr) {
      console.log('Alcancado limite de 91 dias (' + ultimoData + '). Encerrando.');
      break;
    }

    if (dados.length < 100) {
      console.log('Ultima pagina atingida.');
      break;
    }

    p++;
  }

  console.log('Total: ' + allVendas.length + ' pedidos em ' + p + ' paginas.');
  return allVendas;
}

async function main() {
  console.log('Suplemind v4 - ' + new Date().toLocaleString('pt-BR', { timeZone: 'America/Sao_Paulo' }));

  var token = await getAccessToken();
  var allVendas = await fetchAllVendas(token);

  console.log('Coletando produtos...');
  var produtos = await blingGet(token, 'produtos?limite=100&pagina=1');
  var produtosData = (produtos && produtos.data) ? produtos.data : [];

  console.log('Coletando contas a pagar...');
  var cp = await blingGet(token, 'contas/pagar?limite=100&pagina=1');
  var cpData = (cp && cp.data) ? cp.data : [];

  console.log('Coletando contas a receber...');
  var cr = await blingGet(token, 'contas/receber?limite=100&pagina=1');
  var crData = (cr && cr.data) ? cr.data : [];

  var porData = {};
  var porCanal = {};
  var faturamento = 0;

  allVendas.forEach(function(v) {
    var val = parseFloat(v.totalVenda || v.total) || 0;
    faturamento += val;
    var d = (v.data || v.dataVenda || '').substring(0, 10);
    if (d) {
      if (!porData[d]) porData[d] = { count: 0, valor: 0 };
      porData[d].count++;
      porData[d].valor += val;
    }
    var canal = v.loja && v.loja.id ? 'Loja ' + v.loja.id : 'Direto';
    porCanal[canal] = (porCanal[canal] || 0) + 1;
  });

  var comValor = allVendas.filter(function(v) { return (parseFloat(v.totalVenda || v.total) || 0) > 0; });
  var ticket = comValor.length > 0 ? faturamento / comValor.length : 0;
  var totalPagar = cpData.reduce(function(s, c) { return s + (parseFloat(c.valor) || 0); }, 0);
  var totalReceber = crData.reduce(function(s, c) { return s + (parseFloat(c.valor) || 0); }, 0);

  var output = {
    meta: {
      lastUpdate: new Date().toISOString(),
      source: 'Bling API v3',
      collectorVersion: '4.0',
      totalPedidosColetados: allVendas.length
    },
    stats: {
      totalVendas: allVendas.length,
      faturamentoTotal: parseFloat(faturamento.toFixed(2)),
      ticketMedio: parseFloat(ticket.toFixed(2)),
      totalAPagar: parseFloat(totalPagar.toFixed(2)),
      totalAReceber: parseFloat(totalReceber.toFixed(2)),
      porData: Object.entries(porData).sort(function(a, b) { return a[0].localeCompare(b[0]); }),
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
    contasPagar: cpData.slice(0, 100).map(function(c) {
      return { id: c.id, descricao: c.descricao, vencimento: c.dataVencimento, valor: parseFloat(c.valor) || 0, situacao: c.situacao && c.situacao.valor };
    }),
    contasReceber: crData.slice(0, 100).map(function(c) {
      return { id: c.id, descricao: c.descricao, vencimento: c.dataVencimento, valor: parseFloat(c.valor) || 0, situacao: c.situacao && c.situacao.valor };
    })
  };

  var dataDir = path.join(__dirname, 'data');
  if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir, { recursive: true });
  fs.writeFileSync(path.join(dataDir, 'bling.json'), JSON.stringify(output, null, 2));

  console.log('Coleta concluida! ' + allVendas.length + ' pedidos | R$ ' + output.stats.faturamentoTotal);
}

main().catch(function(err) {
  console.error('ERRO CRITICO:', err.message);
  console.error(err.stack);
  process.exit(1);
});
