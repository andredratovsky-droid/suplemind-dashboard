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
  console.log('Token obtido com sucesso');
  if (res.data.refresh_token) {
    fs.writeFileSync('.bling_refresh_token', res.data.refresh_token);
    console.log('Novo refresh token salvo');
  }
  return res.data.access_token;
}

async function blingGet(token, path_) {
  await sleep(400);
  var res = await httpRequest({
    hostname: 'www.bling.com.br',
    path: '/Api/v3/' + path_,
    method: 'GET',
    headers: { 'Authorization': 'Bearer ' + token, 'Accept': 'application/json' }
  });
  return res.data;
}

async function main() {
  console.log('Suplemind - coleta iniciada');
  console.log(new Date().toLocaleString('pt-BR', { timeZone: 'America/Sao_Paulo' }));

  var token = await getAccessToken();

  console.log('Coletando pedidos/vendas...');
  var vendas = await blingGet(token, 'pedidos/vendas?limite=100&pagina=1');
  var vendasData = (vendas && vendas.data) ? vendas.data : [];
  console.log(vendasData.length + ' vendas coletadas');

  console.log('Coletando produtos...');
  var produtos = await blingGet(token, 'produtos?limite=100&pagina=1');
  var produtosData = (produtos && produtos.data) ? produtos.data : [];
  console.log(produtosData.length + ' produtos coletados');

  console.log('Coletando contas a pagar...');
  var contasPagar = await blingGet(token, 'contas/pagar?limite=50&pagina=1');
  var cpData = (contasPagar && contasPagar.data) ? contasPagar.data : [];

  console.log('Coletando contas a receber...');
  var contasReceber = await blingGet(token, 'contas/receber?limite=50&pagina=1');
  var crData = (contasReceber && contasReceber.data) ? contasReceber.data : [];

  var faturamento = vendasData.reduce(function(s, v) {
    return s + (parseFloat(v.totalVenda || v.total) || 0);
  }, 0);
  var comValor = vendasData.filter(function(v) {
    return (parseFloat(v.totalVenda || v.total) || 0) > 0;
  });
  var ticket = comValor.length > 0 ? faturamento / comValor.length : 0;

  var porData = {};
  vendasData.forEach(function(v) {
    var d = (v.data || v.dataVenda || '').substring(0, 10);
    if (d) {
      if (!porData[d]) porData[d] = { count: 0, valor: 0 };
      porData[d].count++;
      porData[d].valor += (parseFloat(v.totalVenda || v.total) || 0);
    }
  });

  var porCanal = {};
  vendasData.forEach(function(v) {
    var nome = v.loja && v.loja.id ? 'Loja ' + v.loja.id : 'Direto';
    porCanal[nome] = (porCanal[nome] || 0) + 1;
  });

  var totalPagar = cpData.reduce(function(s, c) { return s + (parseFloat(c.valor) || 0); }, 0);
  var totalReceber = crData.reduce(function(s, c) { return s + (parseFloat(c.valor) || 0); }, 0);

  var output = {
    meta: {
      lastUpdate: new Date().toISOString(),
      source: 'Bling API v3',
      period: 'ultimos 100 pedidos',
      collectorVersion: '3.0'
    },
    stats: {
      totalVendas: vendasData.length,
      faturamentoTotal: parseFloat(faturamento.toFixed(2)),
      ticketMedio: parseFloat(ticket.toFixed(2)),
      totalAPagar: parseFloat(totalPagar.toFixed(2)),
      totalAReceber: parseFloat(totalReceber.toFixed(2)),
      porData: Object.entries(porData).sort(function(a, b) { return a[0].localeCompare(b[0]); }),
      porCanal: porCanal
    },
    vendas: vendasData.map(function(v) {
      return {
        id: v.id,
        numero: v.numero,
        data: v.data || v.dataVenda,
        contato: (v.contato && v.contato.nome) || (v.cliente && v.cliente.nome) || '',
        total: parseFloat(v.totalVenda || v.total) || 0,
        totalProdutos: parseFloat(v.totalProdutos) || 0,
        situacao: v.situacao && v.situacao.valor,
        lojaId: v.loja && v.loja.id
      };
    }),
    produtos: produtosData.map(function(p) {
      return {
        id: p.id,
        nome: p.nome,
        codigo: p.codigo,
        preco: parseFloat(p.preco) || 0,
        estoque: p.estoque && p.estoque.saldoVirtualTotal
      };
    }),
    contasPagar: cpData.slice(0, 50).map(function(c) {
      return {
        id: c.id,
        descricao: c.descricao,
        vencimento: c.dataVencimento,
        valor: parseFloat(c.valor) || 0,
        situacao: c.situacao && c.situacao.valor
      };
    }),
    contasReceber: crData.slice(0, 50).map(function(c) {
      return {
        id: c.id,
        descricao: c.descricao,
        vencimento: c.dataVencimento,
        valor: parseFloat(c.valor) || 0,
        situacao: c.situacao && c.situacao.valor
      };
    })
  };

  var dataDir = path.join(__dirname, 'data');
  if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir, { recursive: true });
  fs.writeFileSync(path.join(dataDir, 'bling.json'), JSON.stringify(output, null, 2));

  console.log('Coleta concluida com sucesso!');
  console.log(output.stats.totalVendas + ' vendas | R$ ' + output.stats.faturamentoTotal);
  console.log(output.produtos.length + ' produtos');
  console.log('A pagar: R$ ' + output.stats.totalAPagar + ' | A receber: R$ ' + output.stats.totalAReceber);
}

main().catch(function(err) {
  console.error('ERRO CRITICO:', err.message);
  console.error(err.stack);
  process.exit(1);
});
