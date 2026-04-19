// test-meta.js — Teste de credenciais Meta Ads (não imprime valores sensíveis)

const https = require('https');

const META_APP_ID = process.env.META_APP_ID;
const META_APP_SECRET = process.env.META_APP_SECRET;
const META_SYSTEM_USER_TOKEN = process.env.META_SYSTEM_USER_TOKEN;
const META_AD_ACCOUNT_IDS = process.env.META_AD_ACCOUNT_IDS || '';

const GRAPH_API_VERSION = 'v23.0';
const GRAPH_BASE = 'graph.facebook.com';

function mask(value, keepStart, keepEnd) {
  if (!value) return '(vazio)';
  keepStart = keepStart || 4;
  keepEnd = keepEnd || 4;
  if (value.length <= keepStart + keepEnd) return value.slice(0, 2) + '***';
  return value.slice(0, keepStart) + '...(' + (value.length - keepStart - keepEnd) + ' chars)...' + value.slice(-keepEnd);
}

function httpsGet(path) {
  return new Promise(function (resolve, reject) {
    const options = {
      hostname: GRAPH_BASE,
      path: path,
      method: 'GET',
      headers: { 'Accept': 'application/json' }
    };
    const req = https.request(options, function (res) {
      let body = '';
      res.on('data', function (c) { body += c; });
      res.on('end', function () {
        resolve({ status: res.statusCode, body: body });
      });
    });
    req.on('error', reject);
    req.end();
  });
}

function safeParse(body) {
  try { return JSON.parse(body); } catch (e) { return { _rawBody: body }; }
}

async function main() {
  console.log('🔍 Teste de credenciais Meta Ads');
  console.log('   Graph API: ' + GRAPH_API_VERSION);
  console.log('');

  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log('1️⃣  VERIFICANDO SECRETS NO AMBIENTE');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');

  const secrets = {
    META_APP_ID: META_APP_ID,
    META_APP_SECRET: META_APP_SECRET,
    META_SYSTEM_USER_TOKEN: META_SYSTEM_USER_TOKEN,
    META_AD_ACCOUNT_IDS: META_AD_ACCOUNT_IDS
  };

  let todosExistem = true;
  for (const [nome, valor] of Object.entries(secrets)) {
    if (!valor) {
      console.log('   ❌ ' + nome + ': AUSENTE');
      todosExistem = false;
    } else {
      const len = valor.length;
      if (nome === 'META_APP_ID') {
        console.log('   ✅ ' + nome + ': ' + valor + ' (' + len + ' chars)');
      } else if (nome === 'META_AD_ACCOUNT_IDS') {
        console.log('   ✅ ' + nome + ': ' + valor + ' (' + len + ' chars)');
      } else {
        console.log('   ✅ ' + nome + ': ' + mask(valor) + ' (' + len + ' chars)');
      }
    }
  }

  if (!todosExistem) {
    console.log('');
    console.log('⚠️  Faltam Secrets.');
    process.exit(1);
  }

  console.log('');
  if (META_APP_ID.match(/^[0-9]+$/)) console.log('   ✅ App ID formato válido');
  if (META_SYSTEM_USER_TOKEN.length < 50) console.log('   ⚠️  Token parece curto');
  else if (META_SYSTEM_USER_TOKEN.startsWith('EAA')) console.log('   ✅ Token começa com "EAA"');

  const adAccounts = META_AD_ACCOUNT_IDS.split(',').map(function (s) { return s.trim(); }).filter(Boolean);
  console.log('   ℹ️  Ad Accounts: ' + adAccounts.length + ' → [' + adAccounts.join(', ') + ']');

  console.log('');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log('2️⃣  VALIDANDO TOKEN (GET /me)');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');

  const meResp = await httpsGet('/' + GRAPH_API_VERSION + '/me?access_token=' + encodeURIComponent(META_SYSTEM_USER_TOKEN));
  if (meResp.status === 200) {
    const me = safeParse(meResp.body);
    console.log('   ✅ Token válido');
    console.log('   ℹ️  ID: ' + (me.id || '?'));
    if (me.name) console.log('   ℹ️  Nome: ' + me.name);
  } else {
    const err = safeParse(meResp.body);
    console.log('   ❌ Token inválido (HTTP ' + meResp.status + ')');
    if (err.error) {
      console.log('      Tipo: ' + err.error.type);
      console.log('      Mensagem: ' + err.error.message);
      if (err.error.code === 190) console.log('      → Código 190 = token expirado/revogado');
    }
    process.exit(1);
  }

  console.log('');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log('3️⃣  INSPECIONANDO TOKEN (debug_token)');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');

  const appToken = META_APP_ID + '|' + META_APP_SECRET;
  const debugPath = '/' + GRAPH_API_VERSION + '/debug_token?input_token=' +
    encodeURIComponent(META_SYSTEM_USER_TOKEN) +
    '&access_token=' + encodeURIComponent(appToken);
  const debugResp = await httpsGet(debugPath);

  if (debugResp.status === 200) {
    const dbg = safeParse(debugResp.body);
    const d = dbg.data || {};
    console.log('   ✅ Debug realizado');
    console.log('   ℹ️  Válido?: ' + (d.is_valid ? '✅ sim' : '❌ não'));
    console.log('   ℹ️  Tipo: ' + (d.type || '?'));
    console.log('   ℹ️  App ID no token: ' + (d.app_id || '?'));
    if (String(d.app_id) !== META_APP_ID) {
      console.log('      ⚠️  App ID não bate!');
    } else {
      console.log('      ✅ App ID bate com META_APP_ID');
    }
    if (d.expires_at === 0 || !d.expires_at) {
      console.log('   ✅ Token PERMANENTE (System User token)');
    } else {
      const expDate = new Date(d.expires_at * 1000);
      const dias = Math.round((expDate - new Date()) / 86400000);
      console.log('   ⚠️  Token expira em ' + expDate.toISOString() + ' (' + dias + ' dias)');
    }
    if (d.scopes && d.scopes.length > 0) {
      console.log('   ℹ️  Escopos: ' + d.scopes.join(', '));
      if (d.scopes.indexOf('ads_read') !== -1) console.log('      ✅ ads_read presente');
      else console.log('      ❌ ads_read AUSENTE');
      if (d.scopes.indexOf('ads_management') !== -1) console.log('      ⚠️  ads_management presente (BI não precisa)');
    }
  } else {
    console.log('   ⚠️  Falha debug_token (HTTP ' + debugResp.status + ')');
    const err = safeParse(debugResp.body);
    if (err.error) console.log('      Erro: ' + err.error.message);
  }

  console.log('');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log('4️⃣  TESTANDO ACESSO ÀS AD ACCOUNTS');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');

  for (const accId of adAccounts) {
    const path = '/' + GRAPH_API_VERSION + '/act_' + accId +
      '?fields=name,currency,account_status,timezone_name' +
      '&access_token=' + encodeURIComponent(META_SYSTEM_USER_TOKEN);
    const resp = await httpsGet(path);
    if (resp.status === 200) {
      const data = safeParse(resp.body);
      console.log('   ✅ act_' + accId);
      console.log('      Nome: ' + (data.name || '?'));
      console.log('      Moeda: ' + (data.currency || '?'));
      console.log('      Status: ' + (data.account_status === 1 ? 'ACTIVE' : 'STATUS=' + data.account_status));
      console.log('      Timezone: ' + (data.timezone_name || '?'));
    } else {
      const err = safeParse(resp.body);
      console.log('   ❌ act_' + accId + ' — HTTP ' + resp.status);
      if (err.error) console.log('      ' + err.error.message);
    }
  }

  console.log('');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log('5️⃣  TESTE DE INSIGHTS (últimos 7 dias)');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');

  for (const accId of adAccounts) {
    const path = '/' + GRAPH_API_VERSION + '/act_' + accId + '/insights' +
      '?fields=spend,impressions,clicks&date_preset=last_7d' +
      '&access_token=' + encodeURIComponent(META_SYSTEM_USER_TOKEN);
    const resp = await httpsGet(path);
    if (resp.status === 200) {
      const data = safeParse(resp.body);
      const ins = (data.data && data.data[0]) || {};
      console.log('   ✅ act_' + accId + ' (7d)');
      console.log('      Spend: ' + (ins.spend || '0'));
      console.log('      Impressions: ' + (ins.impressions || '0'));
      console.log('      Clicks: ' + (ins.clicks || '0'));
    } else {
      const err = safeParse(resp.body);
      console.log('   ❌ act_' + accId + ' — ' + resp.status);
      if (err.error) console.log('      ' + err.error.message);
    }
  }

  console.log('');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log('✅ FIM DO TESTE');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
}

main().catch(function (err) {
  console.error('❌ ERRO: ' + err.message);
  console.error(err.stack);
  process.exit(1);
});
