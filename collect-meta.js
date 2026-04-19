// collect-meta.js v1.0 — Coletor Meta Ads (Graph API v23.0)
//
// Modos (via env COLLECT_MODE):
//   - "full"        : 180 dias conta/campaign/adset + 30 dias ad
//   - "incremental" : últimos 3 dias em todos os níveis
//
// Output: data/meta.json
// Credenciais: lidas de secrets GitHub (nunca logadas)

const fs = require('fs');
const path = require('path');
const https = require('https');

// ============================================================
// CONFIG
// ============================================================
const MODE = (process.env.COLLECT_MODE || 'full').toLowerCase();
const DATA_DIR = path.join(__dirname, 'data');
const DATA_FILE = path.join(DATA_DIR, 'meta.json');

const META_APP_ID = process.env.META_APP_ID;
const META_APP_SECRET = process.env.META_APP_SECRET;
const META_SYSTEM_USER_TOKEN = process.env.META_SYSTEM_USER_TOKEN;
const META_AD_ACCOUNT_IDS = process.env.META_AD_ACCOUNT_IDS || '';

const GRAPH_API_VERSION = 'v23.0';
const GRAPH_HOST = 'graph.facebook.com';
const RATE_LIMIT_MS = 300;  // Meta permite 200 req/h por app — folgado

const DAYS_FULL = 180;
const DAYS_AD_LEVEL = 30;  // Ads: só últimos 30d mesmo em modo full
const DAYS_INCREMENTAL = 3;

// Renomeação de contas (alias curto para o dashboard)
const ACCOUNT_ALIASES = {
  '929553552297011': 'Principal',
  '1169095775160589': 'Secundária'
};

// Campos de insights (mesmos em todos os níveis)
const INSIGHT_FIELDS = [
  'spend',
  'impressions',
  'clicks',
  'ctr',
  'cpc',
  'cpm',
  'reach',
  'frequency',
  'actions',         // lista de {action_type, value} — onde mora o purchase count
  'action_values'    // lista de {action_type, value} — onde mora o purchase value (receita Meta)
].join(',');

// ============================================================
// HTTP helpers
// ============================================================
function httpsGet(pathWithQuery) {
  return new Promise(function (resolve, reject) {
    const req = https.request({
      hostname: GRAPH_HOST,
      path: pathWithQuery,
      method: 'GET',
      headers: { 'Accept': 'application/json' }
    }, function (res) {
      let body = '';
      res.on('data', function (c) { body += c; });
      res.on('end', function () {
        resolve({ status: res.statusCode, body: body, headers: res.headers });
      });
    });
    req.on('error', reject);
    req.end();
  });
}

function sleep(ms) { return new Promise(function (r) { setTimeout(r, ms); }); }

function safeParse(body) {
  try { return JSON.parse(body); } catch (e) { return { _raw: body }; }
}

// Graph API call com retry automático em rate limit
function graphGet(endpoint, queryParams) {
  const qs = new URLSearchParams(queryParams || {});
  qs.set('access_token', META_SYSTEM_USER_TOKEN);
  const fullPath = '/' + GRAPH_API_VERSION + endpoint + '?' + qs.toString();

  return httpsGet(fullPath).then(function (resp) {
    if (resp.status === 200) {
      return safeParse(resp.body);
    }
    const err = safeParse(resp.body);
    const errObj = err.error || {};
    // Rate limit: wait and retry
    if (errObj.code === 17 || errObj.code === 613 || errObj.code === 4 || resp.status === 429) {
      console.log('  ⏳ Rate limit atingido, aguardando 60s...');
      return sleep(60000).then(function () { return graphGet(endpoint, queryParams); });
    }
    throw new Error('Graph ' + endpoint + ' HTTP ' + resp.status + ': ' + (errObj.message || resp.body.slice(0, 200)));
  });
}

// ============================================================
// Paginação: segue cursor next até acabar
// ============================================================
function graphPaginate(endpoint, queryParams, label) {
  const all = [];
  let pageCount = 0;

  function next(qParams, nextUrl) {
    pageCount += 1;
    let promise;

    if (nextUrl) {
      // Use a URL "next" já formada pelo Graph
      const url = new URL(nextUrl);
      const qs = url.search || '';
      // Garantir que access_token esteja presente
      const params = new URLSearchParams(qs);
      if (!params.get('access_token')) params.set('access_token', META_SYSTEM_USER_TOKEN);
      promise = httpsGet(url.pathname + '?' + params.toString()).then(function (resp) {
        if (resp.status !== 200) {
          const err = safeParse(resp.body);
          throw new Error('Paginação ' + label + ' HTTP ' + resp.status + ': ' + ((err.error && err.error.message) || resp.body.slice(0, 150)));
        }
        return safeParse(resp.body);
      });
    } else {
      promise = graphGet(endpoint, qParams);
    }

    return promise.then(function (resp) {
      const items = (resp && resp.data) || [];
      all.push.apply(all, items);
      if (pageCount === 1 || pageCount % 5 === 0) {
        console.log('    📄 ' + label + ' pág ' + pageCount + ': +' + items.length + ' (total: ' + all.length + ')');
      }
      if (resp.paging && resp.paging.next) {
        return sleep(RATE_LIMIT_MS).then(function () { return next(null, resp.paging.next); });
      }
      return all;
    });
  }

  return next(queryParams, null);
}

// ============================================================
// Coleta de metadados da conta (nome, currency, timezone, etc)
// ============================================================
function coletarAccountMeta(accountId) {
  const endpoint = '/act_' + accountId;
  return graphGet(endpoint, {
    fields: 'id,account_id,name,currency,timezone_name,account_status,amount_spent,business_name'
  });
}

// ============================================================
// Coleta insights em um nível específico
// level: "account" | "campaign" | "adset" | "ad"
// ============================================================
function coletarInsights(accountId, level, days) {
  const since = new Date(Date.now() - days * 86400000).toISOString().slice(0, 10);
  const until = new Date().toISOString().slice(0, 10);
  const timeRange = JSON.stringify({ since: since, until: until });

  const params = {
    fields: INSIGHT_FIELDS,
    level: level,
    time_range: timeRange,
    time_increment: '1',  // Quebra por dia
    limit: '500'          // Máximo por página (reduz nº de paginações)
  };

  const label = 'insights[' + level + '] act_' + accountId.slice(-4) + ' (' + days + 'd)';
  console.log('  🔹 ' + label);

  return graphPaginate('/act_' + accountId + '/insights', params, label);
}

// ============================================================
// Extrator de actions/action_values — converte os arrays do Meta
// em campos planos (purchases, purchase_value)
// ============================================================
function enriquecerInsight(row) {
  const enriched = Object.assign({}, row);

  // Purchases (conta)
  const actions = row.actions || [];
  let purchases = 0;
  let addToCart = 0;
  let initiatedCheckout = 0;
  actions.forEach(function (a) {
    const v = parseFloat(a.value) || 0;
    // Vários aliases possíveis do pixel/CAPI
    if (a.action_type === 'purchase' || a.action_type === 'omni_purchase' ||
        a.action_type === 'offsite_conversion.fb_pixel_purchase') {
      purchases += v;
    }
    if (a.action_type === 'add_to_cart' || a.action_type === 'offsite_conversion.fb_pixel_add_to_cart') {
      addToCart += v;
    }
    if (a.action_type === 'initiate_checkout' || a.action_type === 'offsite_conversion.fb_pixel_initiate_checkout') {
      initiatedCheckout += v;
    }
  });

  // Purchase value (receita)
  const actionValues = row.action_values || [];
  let purchaseValue = 0;
  actionValues.forEach(function (av) {
    const v = parseFloat(av.value) || 0;
    if (av.action_type === 'purchase' || av.action_type === 'omni_purchase' ||
        av.action_type === 'offsite_conversion.fb_pixel_purchase') {
      purchaseValue += v;
    }
  });

  enriched.purchases = purchases;
  enriched.addToCart = addToCart;
  enriched.initiatedCheckout = initiatedCheckout;
  enriched.purchaseValue = purchaseValue;

  const spend = parseFloat(row.spend) || 0;
  enriched.spend = spend;
  enriched.roas = spend > 0 ? purchaseValue / spend : 0;
  enriched.cpa = purchases > 0 ? spend / purchases : 0;

  return enriched;
}

// ============================================================
// Cálculo de estatísticas consolidadas
// ============================================================
function computeStats(accountInsights, allCampaignInsights) {
  const hoje = new Date();
  const d7 = new Date(hoje.getTime() - 7 * 86400000);
  const d30 = new Date(hoje.getTime() - 30 * 86400000);
  const d90 = new Date(hoje.getTime() - 90 * 86400000);

  function aggregate(rows, desde) {
    const filtered = desde ? rows.filter(function (r) {
      return new Date(r.date_start || 0) >= desde;
    }) : rows;
    let spend = 0, purchases = 0, purchaseValue = 0, impressions = 0, clicks = 0;
    filtered.forEach(function (r) {
      spend += parseFloat(r.spend) || 0;
      purchases += r.purchases || 0;
      purchaseValue += r.purchaseValue || 0;
      impressions += parseInt(r.impressions) || 0;
      clicks += parseInt(r.clicks) || 0;
    });
    return {
      spend: spend,
      purchases: purchases,
      purchaseValue: purchaseValue,
      impressions: impressions,
      clicks: clicks,
      roas: spend > 0 ? purchaseValue / spend : 0,
      cpa: purchases > 0 ? spend / purchases : 0,
      ctr: impressions > 0 ? (clicks / impressions) * 100 : 0
    };
  }

  // Top campanhas por spend e por ROAS
  // Agrega insights de campanha por campaign_id
  const byCampaign = {};
  allCampaignInsights.forEach(function (r) {
    const id = r.campaign_id || 'unknown';
    if (!byCampaign[id]) {
      byCampaign[id] = {
        campaign_id: id,
        campaign_name: r.campaign_name || '(sem nome)',
        account_id: r.account_id,
        spend: 0, purchases: 0, purchaseValue: 0
      };
    }
    byCampaign[id].spend += parseFloat(r.spend) || 0;
    byCampaign[id].purchases += r.purchases || 0;
    byCampaign[id].purchaseValue += r.purchaseValue || 0;
  });
  const campaignArr = Object.values(byCampaign).map(function (c) {
    c.roas = c.spend > 0 ? c.purchaseValue / c.spend : 0;
    c.cpa = c.purchases > 0 ? c.spend / c.purchases : 0;
    return c;
  });

  const topBySpend = campaignArr.slice().sort(function (a, b) { return b.spend - a.spend; }).slice(0, 20);
  const topByRoas = campaignArr
    .filter(function (c) { return c.spend >= 100; })  // Filtrar campanhas com spend relevante
    .sort(function (a, b) { return b.roas - a.roas; })
    .slice(0, 20);

  return {
    consolidated: {
      '7d': aggregate(accountInsights, d7),
      '30d': aggregate(accountInsights, d30),
      '90d': aggregate(accountInsights, d90),
      total: aggregate(accountInsights, null)
    },
    totalCampaigns: campaignArr.length,
    topCampaignsBySpend: topBySpend,
    topCampaignsByRoas: topByRoas
  };
}

// ============================================================
// MAIN
// ============================================================
async function main() {
  console.log('🚀 Suplemind Meta Ads Collector v1.0');
  console.log('   Modo: ' + MODE);
  console.log('   Timestamp: ' + new Date().toISOString());
  console.log('   Graph API: ' + GRAPH_API_VERSION);
  console.log('');

  if (!META_APP_ID || !META_APP_SECRET || !META_SYSTEM_USER_TOKEN || !META_AD_ACCOUNT_IDS) {
    throw new Error('Secrets Meta ausentes — configure META_APP_ID, META_APP_SECRET, META_SYSTEM_USER_TOKEN, META_AD_ACCOUNT_IDS');
  }

  const accountIds = META_AD_ACCOUNT_IDS.split(',').map(function (s) { return s.trim(); }).filter(Boolean);
  console.log('📋 Ad Accounts: ' + accountIds.length + ' (' + accountIds.map(function (a) { return 'act_' + a; }).join(', ') + ')');
  console.log('');

  // Determinar janelas de tempo baseado no modo
  const daysAccount = MODE === 'incremental' ? DAYS_INCREMENTAL : DAYS_FULL;
  const daysCampaign = MODE === 'incremental' ? DAYS_INCREMENTAL : DAYS_FULL;
  const daysAdset = MODE === 'incremental' ? DAYS_INCREMENTAL : DAYS_FULL;
  const daysAd = MODE === 'incremental' ? DAYS_INCREMENTAL : DAYS_AD_LEVEL;

  console.log('🗓️  Janelas:');
  console.log('   Account: ' + daysAccount + ' dias');
  console.log('   Campaign: ' + daysCampaign + ' dias');
  console.log('   Adset: ' + daysAdset + ' dias');
  console.log('   Ad: ' + daysAd + ' dias');
  console.log('');

  // Estruturas de output
  const accountsMeta = {};
  const insights = {
    account: [],
    campaign: [],
    adset: [],
    ad: []
  };

  // Loop por conta
  for (const accId of accountIds) {
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    console.log('📊 Processando act_' + accId);
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');

    // 1. Metadata
    console.log('  📌 Coletando metadata...');
    const meta = await coletarAccountMeta(accId);
    accountsMeta['act_' + accId] = {
      id: meta.id,
      account_id: meta.account_id,
      name: meta.name || '',
      business_name: meta.business_name || '',
      currency: meta.currency || '',
      timezone: meta.timezone_name || '',
      account_status: meta.account_status,
      amount_spent_total: meta.amount_spent || '0',
      alias: ACCOUNT_ALIASES[accId] || ('Conta ' + accId.slice(-4))
    };
    console.log('    ✅ ' + meta.name + ' (alias: ' + accountsMeta['act_' + accId].alias + ')');
    await sleep(RATE_LIMIT_MS);

    // 2. Insights nível conta
    console.log('  📈 Coletando insights (nível conta)...');
    const accRaw = await coletarInsights(accId, 'account', daysAccount);
    const accEnriched = accRaw.map(enriquecerInsight).map(function (r) {
      r.account_id = accId;
      return r;
    });
    insights.account.push.apply(insights.account, accEnriched);
    console.log('    ✅ ' + accEnriched.length + ' linhas (conta×dia)');
    await sleep(RATE_LIMIT_MS);

    // 3. Insights nível campanha
    console.log('  📈 Coletando insights (nível campanha)...');
    const campRaw = await coletarInsights(accId, 'campaign', daysCampaign);
    const campEnriched = campRaw.map(enriquecerInsight).map(function (r) {
      r.account_id = accId;
      return r;
    });
    insights.campaign.push.apply(insights.campaign, campEnriched);
    console.log('    ✅ ' + campEnriched.length + ' linhas (campanha×dia)');
    await sleep(RATE_LIMIT_MS);

    // 4. Insights nível adset
    console.log('  📈 Coletando insights (nível adset)...');
    const adsetRaw = await coletarInsights(accId, 'adset', daysAdset);
    const adsetEnriched = adsetRaw.map(enriquecerInsight).map(function (r) {
      r.account_id = accId;
      return r;
    });
    insights.adset.push.apply(insights.adset, adsetEnriched);
    console.log('    ✅ ' + adsetEnriched.length + ' linhas (adset×dia)');
    await sleep(RATE_LIMIT_MS);

    // 5. Insights nível ad (só últimos 30d em full mode)
    console.log('  📈 Coletando insights (nível ad)...');
    const adRaw = await coletarInsights(accId, 'ad', daysAd);
    const adEnriched = adRaw.map(enriquecerInsight).map(function (r) {
      r.account_id = accId;
      return r;
    });
    insights.ad.push.apply(insights.ad, adEnriched);
    console.log('    ✅ ' + adEnriched.length + ' linhas (ad×dia)');
    await sleep(RATE_LIMIT_MS);
  }

  console.log('');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log('📊 Calculando estatísticas consolidadas...');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');

  const stats = computeStats(insights.account, insights.campaign);

  const output = {
    meta: {
      version: '1.0',
      collectedAt: new Date().toISOString(),
      mode: MODE,
      graphApiVersion: GRAPH_API_VERSION,
      windows: {
        account: daysAccount,
        campaign: daysCampaign,
        adset: daysAdset,
        ad: daysAd
      },
      counts: {
        accounts: Object.keys(accountsMeta).length,
        accountInsights: insights.account.length,
        campaignInsights: insights.campaign.length,
        adsetInsights: insights.adset.length,
        adInsights: insights.ad.length
      }
    },
    accounts: accountsMeta,
    insights: insights,
    stats: stats
  };

  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  fs.writeFileSync(DATA_FILE, JSON.stringify(output, null, 2));

  const sizeKB = (fs.statSync(DATA_FILE).size / 1024).toFixed(1);

  console.log('');
  console.log('✅ Coleta Meta concluída!');
  console.log('   Arquivo: ' + sizeKB + ' KB');
  console.log('');
  console.log('📈 RESUMO CONSOLIDADO:');
  console.log('   Total 7d:   R$ ' + stats.consolidated['7d'].spend.toFixed(2) +
              ' | Purchases: ' + stats.consolidated['7d'].purchases +
              ' | Revenue: R$ ' + stats.consolidated['7d'].purchaseValue.toFixed(2) +
              ' | ROAS: ' + stats.consolidated['7d'].roas.toFixed(2) + 'x');
  console.log('   Total 30d:  R$ ' + stats.consolidated['30d'].spend.toFixed(2) +
              ' | Purchases: ' + stats.consolidated['30d'].purchases +
              ' | Revenue: R$ ' + stats.consolidated['30d'].purchaseValue.toFixed(2) +
              ' | ROAS: ' + stats.consolidated['30d'].roas.toFixed(2) + 'x');
  console.log('   Total 90d:  R$ ' + stats.consolidated['90d'].spend.toFixed(2) +
              ' | Purchases: ' + stats.consolidated['90d'].purchases +
              ' | Revenue: R$ ' + stats.consolidated['90d'].purchaseValue.toFixed(2) +
              ' | ROAS: ' + stats.consolidated['90d'].roas.toFixed(2) + 'x');
  console.log('   Total geral: R$ ' + stats.consolidated.total.spend.toFixed(2) +
              ' | Purchases: ' + stats.consolidated.total.purchases +
              ' | Revenue: R$ ' + stats.consolidated.total.purchaseValue.toFixed(2) +
              ' | ROAS: ' + stats.consolidated.total.roas.toFixed(2) + 'x');
  console.log('');
  console.log('🎯 Campanhas únicas: ' + stats.totalCampaigns);
  console.log('   Top 3 por spend:');
  stats.topCampaignsBySpend.slice(0, 3).forEach(function (c, i) {
    console.log('     ' + (i + 1) + '. ' + c.campaign_name.slice(0, 50) +
                ' — R$ ' + c.spend.toFixed(2) + ' (ROAS ' + c.roas.toFixed(2) + 'x)');
  });
  console.log('   Top 3 por ROAS (spend >=R$100):');
  stats.topCampaignsByRoas.slice(0, 3).forEach(function (c, i) {
    console.log('     ' + (i + 1) + '. ' + c.campaign_name.slice(0, 50) +
                ' — ROAS ' + c.roas.toFixed(2) + 'x (R$ ' + c.spend.toFixed(2) + ' gasto)');
  });
}

main().catch(function (err) {
  console.error('❌ ERRO: ' + err.message);
  console.error(err.stack);
  process.exit(1);
});
