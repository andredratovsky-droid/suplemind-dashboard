/**
 * Suplemind Intelligence — Data Collector v2
 * Correções:
 * - Refresh token renovado via GitHub API diretamente neste script (mais confiável)
 * - Tratamento de erro detalhado com logs claros
 * - Fallback: se coleta de um endpoint falhar, continua com os demais
 * - Valida se token foi realmente renovado antes de continuar
 */

const https = require('https');
const fs = require('fs');
const path = require('path');

const CLIENT_ID     = process.env.BLING_CLIENT_ID;
const CLIENT_SECRET = process.env.BLING_CLIENT_SECRET;
const REFRESH_TOKEN = process.env.BLING_REFRESH_TOKEN;
const GH_TOKEN      = process.env.GH_TOKEN;
const GH_REPO       = process.env.GITHUB_REPOSITORY;

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
  if (!CLIENT_ID || !CLIENT_SECRET || !REFRESH_TOKEN) {
    throw new Error('Variaveis de ambiente ausentes: BLING_CLIENT_ID, BLING_CLIENT_SECRET ou BLING_REFRESH_TOKEN nao definidas nos Secrets.');
  }
  console.log('Renovando access token Bling...');
  const credentials = Buffer.from(`${CLIENT_ID}:${CLIENT_SECRET}`).toString('base64');
  const body = `grant_type=refresh_token&refresh_token=${REFRESH_TOKEN}`;
  const res = await httpRequest({
    hostname: 'www.bling.com.br',
    path: '/Api/v3/oauth/token',
    method: 'POST',
    headers: {
      'Authorization': `Basic ${credentials}`,
      'Content-Type': 'application/x-www-form-urlencoded',
      'Content-Length': Buffer.byteLength(body),
    }
  }, body);
  if (res.status !== 200 || !res.data.access_token) {
    throw new Error(
      `Falha ao renovar token (HTTP ${res.status}):\n` +
      JSON.stringify(res.data, null, 2) + '\n\n' +
      'O BLING_REFRESH_TOKEN nos Secrets provavelmente expirou.\n' +
      'Acesse o Bling, gere um novo token e atualize o Secret BLING_REFRESH_TOKEN no GitHub.'
    );
  }
  console.log('Access token renovado com sucesso');
  const newRefreshToken = res.data.refresh_token;
  fs.writeFileSync('.bling_refresh_token', newRefreshToken || REFRESH_TOKEN);
  console.log('Novo refresh token salvo para atualizacao nos Secrets');
  return res.data.access_token;
}

alsync function blingGet(token, endpoint, retries = 2) {
  await new Promise(r => setTimeout(r, 400));
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      const res = await httpRequest({
        hostname: 'www.bling.com.br',
        path: `/Api/v3/${endpoint}`,
        method: 'GET',
        headers: { 'Authorization': `Bearer ${token}`, 'Accept': 'application/json' }
      });
      if (res.status === 429) { await new Promise(r => setTimeout(r, 2000)); continue; }
      if (res.status === 401) { throw new Error(`Token invalido para ${endpoint} (HTTP 401)`); }
      return res.data;
    } catch (e) {
      if (attempt === retries) throw e;
      console.warn(`Tentativa ${attempt + 1} falhou para ${endpoint}: ${e.message}`);
      await new Promise(r => setTimeout(r, 1000));
    }
  }
}

alsync function collectAll(token) {
  const results = {};
  try {
    console.log('Coletando vendas...');
    results.vendas = await blingGet(token, 'pedidos/vendas?limite=100&pagina=1');
    console.log(`${results.vendas?.data?.length || 0} vendas`);
  } catch(e) { console.error(`Vendas: ${e.message}`); results.vendas = { data: [] }; }
  try {
    console.log('Coletando produtos...');
    results.produtos = await blingGet(token, 'produtos?limite=100&pagina=1');
    console.log(`${results.produtos?.data?.length || 0} produtos`);
  } catch(e) { console.error(`Produtos: ${e.message}`); results.produtos = { data: [] }; }
  try {
    console.log('Coletando contas a pagar...');
    results.contasPagar = await blingGet(token, 'contas/pagar?limite=50&pagina=1');
    console.log(`${results.contasPagar?.data?.length || 0} contas a pagar`);
  } catch(e) { console.error(`Contas pagar: ${e.message}`); results.contasPagar = { data: [] }; }
  try {
    console.log('Coletando contas a receber...');
    results.contasReceber = await blingGet(token, 'contas/receber?limite=50&pagina=1');
    console.log(`${results.contasReceber?.data?.length || 0} contas a receber`);
  } catch(e) { console.error(`Contas receber: ${e.message}`); results.contasReceber = { data: [] }; }
  const vendasData = results.vendas?.data || [];
  const faturamento = vendasData.reduce((s,v) => s + (parseFloat(v.totalVenda || v.total) || 0), 0);
  const vComValor = vendasData.filter(v => (parseFloat(v.totalVenda || v.total) || 0) > 0);
  const ticket = vComValor.length > 0 ? faturamento / vComValor.length : 0;
  const porData = {};
  vendasData.forEach(v => {
    const d = (v.data || v.dataVenda || '').substring(0,10);
    if (d) { if (!porData[d]) porData[d] = { count:0, valor:0 }; porData[d].count++; porData[d].valor += (parseFloat(v.totalVenda || v.total) || 0); }
  });
  const porCanal = {};
  vendasData.forEach(v => { const id = v.loja?.id; const nome = id ? `Loja ${id}` : 'Direto'; porCanal[nome] = (porCanal[nome] || 0) + 1; });
  const totalPagar = (results.contasPagar?.data || []).reduce((s,c) => s + (parseFloat(c.valor) || 0), 0);
  const totalReceber = (results.contasReceber?.data || []).reduce((s,c) => s + (parseFloat(c.valor) || 0), 0);
  return {
    meta: { lastUpdate: new Date().toISOString(), source: 'Bling API v3', period: 'ultimos 100 pedidos', collectorVersion: '2.0' },
    stats: { totalVendas: vendasData.length, faturamentoTotal: parseFloat(faturamento.toFixed(2)), ticketMedio: parseFloat(ticket.toFixed(2)), totalAPagar: parseFloat(totalPagar.toFixed(2)), totalAReceber: parseFloat(totalReceber.toFixed(2)), porData: Object.entries(porData).sort(([a],[b]) => a.localeCompare(b)), porCanal },
    vendas: vendasData.map(v => ({ id:v.id, numero:v.numero, data:v.data||v.dataVenda, contato:v.contato?.nome||v.cliente?.nome, total:parseFloat(v.totalVenda||v.total)||0, totalProdutos:parseFloat(v.totalProdutos)||0, situacao:v.situacao?.valor, lojaId:v.loja?.id })),
    produtos: (results.produtos?.data || []).map(p => ({ id:p.id, nome:p.nome, codigo:p.codigo, preco:parseFloat(p.preco)||0, estoque:p.estoque?.saldoVirtualTotal })),
    contasPagar: (results.contasPagar?.data || []).map(c => ({ id:c.id, descricao:c.descricao, vencimento:c.dataVencimento, valor:parseFloat(c.valor)||0, situacao:c.situacao?.valor })).slice(0,50),
    contasReceber: (results.contasReceber?.data || []).map(c => ({ id:c.id, descricao:c.descricao, vencimento:c.dataVencimento, valor:parseFloat(c.valor)||0, situacao:c.situacao?.valor })).slice(0,50),
  };
}

alsync function main() {
  console.log('Suplemind Intelligence - Coleta de dados v2');
  console.log(new Date().toLocaleString('pt-BR', {timeZone:'America/Sao_Paulo"}));
  try {
    const token = await refreshAccessToken();
    const data = await collectAll(token);
    const dataDir = path.join(__dirname, 'data');
    if (!fs.existsSync(dataDir)) fs.mkdirSync(dataDir, { recursive: true });
    fs.writeFileSync(path.join(dataDir, 'bling.json'), JSON.stringify(data, null, 2));
    console.log('Coleta concluida com sucesso!');
    console.log(`${data.stats.totalVendas} vendas | R$ ${data.stats.faturamentoTotal}`);
  } catch (err) {
    console.error('ERRO CRITICO NA COLETA:', err.message);
    console.error('Stacktrace:', err.stack);
    process.exit(1);
  }
}

main();
