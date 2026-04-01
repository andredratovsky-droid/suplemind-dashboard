/**
 * Suplemind Intelligence — Data Collector
 * Roda via GitHub Actions 2x por dia (8h e 20h BRT)
 * Coleta dados do Bling API v3 e salva em /data/*.json
 */

const https = require('https');
const fs = require('fs');
const path = require('path');

const CLIENT_ID     = process.env.BLING_CLIENT_ID;
const CLIENT_SECRET = process.env.BLING_CLIENT_SECRET;
const REFRESH_TOKEN = process.env.BLING_REFRESH_TOKEN;

function httpRequest(options, body = null) {
  return new Promise((resolve, reject) => {
    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try { resolve({ status: res.statusCode, data: JSON.parse(data) }); }
        catch(e) { resolve({ status: res.statusCode, data }); }
      });
    });
    req.on('error', reject);
    if (body) req.write(body);
    req.end();
  });
}

async function refreshAccessToken() {
  const credentials = Buffer.from(CLIENT_ID + ':' + CLIENT_SECRET).toString('base64');
  const body = 'grant_type=refresh_token&refresh_token=' + REFRESH_TOKEN;
  const res = await httpRequest({
    hostname: 'www.bling.com.br',
    path: '/Api/v3/oauth/token',
    method: 'POST',
    headers: {
      'Authorization': 'Basic ' + credentials,
      'Content-Type': 'application/x-www-form-urlencoded',
      'Content-Length': Buffer.byteLength(body),
    }
  }, body);
  if (res.data.access_token) {
    console.log('Token renovado com sucesso');
    fs.writeFileSync('.bling_refresh_token', res.data.refresh_token);
    return res.data.access_token;
  } else {
    throw new Error('Falha ao renovar token: ' + JSON.stringify(res.data));
  }
}

async function blingGet(token, endpoint) {
  await new Promise(r => setTimeout(r, 400));
  const res = await httpRequest({
    hostname: 'www.bling.com.br',
    path: '/Api/v3/' + endpoint,
    method: 'GET',
    headers: { 'Authorization': 'Bearer ' + token, 'Accept': 'application/json' }
  });
  return res.data;
}

async function collectAll(token) {
  console.log('Coletando vendas...');
  const vendas = await blingGet(token, 'vendas?limite=100&pagina=1');
  console.log('Coletando produtos...');
  const produtos = await blingGet(token, 'produtos?limite=100&pagina=1');
  console.log('Coletando contas a pagar...');
  const contasPagar = await blingGet(token, 'contas/pagar?limite=100&pagina=1');
  console.log('Coletando contas a receber...');
  const contasReceber = await blingGet(token, 'contas/receber?limite=100&pagina=1');

  const vendasData = vendas?.data || [];
  const faturamento = vendasData.reduce((s, v) => s + (v.total || 0), 0);
  const ticket = faturamento / (vendasData.filter(v => v.total > 0).length || 1);

  const porData = {};
  vendasData.forEach(v => {
    const d = v.data?.substring(0,10);
    if (d) {
      if (!porData[d]) porData[d] = { count: 0, valor: 0 };
      porData[d].count++;
      porData[d].valor += (v.total || 0);
    }
  });

  const porCanal = {};
  vendasData.forEach(v => {
    const id = v.loja?.id;
    const nome = id ? 'Loja ' + id : 'Direto';
    porCanal[nome] = (porCanal[nome] || 0) + 1;
  });

  const totalPagar = (contasPagar?.data || []).reduce((s,c) => s + (c.valor || 0), 0);
  const totalReceber = (contasReceber?.data || []).reduce((s,c) => s + (c.valor || 0), 0);

  return {
    meta: { lastUpdate: new Date().toISOString(), source: 'Bling API v3' },
    stats: {
      totalVendas: vendasData.length,
      faturamentoTotal: parseFloat(faturamento.toFixed(2)),
      ticketMedio: parseFloat(ticket.toFixed(2)),
      totalAPagar: parseFloat(totalPagar.toFixed(2)),
      totalAReceber: parseFloat(totalReceber.toFixed(2)),
      porData: Object.entries(porData).sort(([a],[b]) => a.localeCompare(b)),
      porCanal,
    },
    vendas: vendasData.map(v => ({
      id: v.id, numero: v.numero, data: v.data,
      contato: v.contato?.nome, total: v.total,
      totalProdutos: v.totalProdutos, situacao: v.situacao?.valor, lojaId: v.loja?.id,
    })),
    produtos: (produtos?.data || []).map(p => ({
      id: p.id, nome: p.nome, codigo: p.codigo, preco: p.preco,
    })),
    contasPagar: (contasPagar?.data || []).slice(0, 50).map(c => ({
      id: c.id, descricao: c.descricao, vencimento: c.dataVencimento,
      valor: c.valor, situacao: c.situacao?.valor,
    })),
    contasReceber: (contasReceber?.data || []).slice(0, 50).map(c => ({
      id: c.id, descricao: c.descricao, vencimento: c.dataVencimento,
      valor: c.valor, situacao: c.situacao?.valor,
    })),
  };
}

async function main() {
  try {
    console.log('Suplemind Intelligence — Coleta iniciada');
    const token = await refreshAccessToken();
    const data = await collectAll(token);
    const dataDir = path.join(__dirname, 'data');
    if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir, { recursive: true });
    fs.writeFileSync(path.join(dataDir, 'bling.json'), JSON.stringify(data, null, 2));
    console.log('Dados salvos: ' + data.stats.totalVendas + ' vendas | R$ ' + data.stats.faturamentoTotal);
  } catch (err) {
    console.error('Erro na coleta:', err.message);
    process.exit(1);
  }
}

main();
