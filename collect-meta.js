// collect-meta.js v2.1 — Coletor Meta Ads (Graph API v23.0)
//
// Fixes vs v2.0:
//   - Níveis adset/ad: quebra janela grande em sub-janelas de 30d (evita HTTP 500)
//   - Retry com backoff em erros 5xx (transitórios)
//   - Fallback gracioso: se um nível falhar, continua com os outros
//   - Limite de paginação para evitar loop infinito

const fs = require('fs');
const path = require('path');
const https = require('https');

// ============================================================
// CONFIG
// ============================================================
const MODE = (process.env.COLLECT_MODE || 'full').toLowerCase();
const DATA_DIR = path.join(__dirname, 'data');
const DATA_FILE = path.join(DATA_DIR, 'meta.json');
const CREATIVES_CACHE_FILE = path.join(DATA_DIR, 'meta_creatives_cache.json');

const META_APP_ID = process.env.META_APP_ID;
const META_APP_SECRET = process.env.META_APP_SECRET;
const META_SYSTEM_USER_TOKEN = process.env.META_SYSTEM_USER_TOKEN;
const META_AD_ACCOUNT_IDS = process.env.META_AD_ACCOUNT_IDS || '';

const GRAPH_API_VERSION = 'v23.0';
const GRAPH_HOST = 'graph.facebook.com';
const RATE_LIMIT_MS = 300;
const MAX_PAGES_PER_QUERY = 30;  // teto de páginas por request (evita loop)

const DAYS_FULL = 180;
const DAYS_AD_LEVEL = 30;
const DAYS_INCREMENTAL = 3;
const DAYS_BREAKDOWN = 30;

// Janelas menores para níveis granulares (evita HTTP 500 no Meta)
const CHUNK_DAYS_GRANULAR = 30;  // adset/ad: 180d → 6 chunks de 30d

const ACCOUNT_ALIASES = {
  '929553552297011': 'Principal',
  '1169095775160589': 'Secundária'
};

// ============================================================
// CAMPOS
// ============================================================
const INSIGHT_FIELDS_BASE = [
  'spend', 'impressions', 'clicks', 'ctr', 'cpc', 'cpm', 'cpp', 'reach', 'frequency',
  'inline_link_clicks', 'outbound_clicks', 'unique_clicks', 'unique_ctr',
  'unique_inline_link_clicks', 'unique_link_clicks_ctr',
  'cost_per_inline_link_click', 'cost_per_outbound_click',
  'cost_per_unique_click', 'cost_per_unique_inline_link_click',
  'actions', 'action_values', 'unique_actions',
  'cost_per_action_type', 'cost_per_unique_action_type',
  'conversions', 'conversion_values',
  'inline_post_engagement', 'social_spend',
  'video_play_actions', 'video_thruplay_watched_actions',
  'video_p25_watched_actions', 'video_p50_watched_actions',
  'video_p75_watched_actions', 'video_p100_watched_actions',
  'video_avg_time_watched_actions', 'cost_per_thruplay',
  'quality_ranking', 'engagement_rate_ranking', 'conversion_rate_ranking'
].join(',');

const INSIGHT_FIELDS_ACCOUNT = INSIGHT_FIELDS_BASE
  .split(',')
  .filter(function (f) { return !['quality_ranking', 'engagement_rate_ranking', 'conversion_rate_ranking'].includes(f); })
  .join(',');

// Conjunto reduzido de campos (fallback quando HTTP 500 mesmo em sub-janelas)
const INSIGHT_FIELDS_MINIMAL = [
  'spend', 'impressions', 'clicks', 'ctr', 'reach',
  'actions', 'action_values', 'inline_link_clicks'
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
      res.on('end', function () { resolve({ status: res.statusCode, body: body }); });
    });
    req.on('error', reject);
    req.setTimeout(120000, function () { req.destroy(new Error('Request timeout 120s')); });
    req.end();
  });
}

function sleep(ms) { return new Promise(function (r) { setTimeout(r, ms); }); }
function safeParse(body) { try { return JSON.parse(body); } catch (e) { return { _raw: body }; } }

// graphGet com retry automático em 5xx e rate limit
function graphGet(endpoint, queryParams, retryCount) {
  retryCount = retryCount || 0;
  const qs = new URLSearchParams(queryParams || {});
  qs.set('access_token', META_SYSTEM_USER_TOKEN);
  const fullPath = '/' + GRAPH_API_VERSION + endpoint + '?' + qs.toString();

  return httpsGet(fullPath).then(function (resp) {
    if (resp.status === 200) return safeParse(resp.body);

    const err = safeParse(resp.body);
    const errObj = err.error || {};

    // Rate limit
    if (errObj.code === 17 || errObj.code === 613 || errObj.code === 4 || resp.status === 429) {
      if (retryCount >= 3) throw new Error('Rate limit persistente após 3 tentativas');
      console.log('  ⏳ Rate limit (tentativa ' + (retryCount + 1) + '/3), aguardando 60s...');
      return sleep(60000).then(function () { return graphGet(endpoint, queryParams, retryCount + 1); });
    }

    // Breakdown não suportado: retorna vazio
    if (errObj.code === 100 && errObj.message && errObj.message.indexOf('breakdown') !== -1) {
      console.log('  ⚠️  Breakdown não suportado neste nível, pulando');
      return { data: [] };
    }

    // HTTP 500/502/503 — transitório, retry com backoff
    if (resp.status >= 500 && resp.status < 600) {
      if (retryCount >= 2) {
        throw new Error('Graph ' + endpoint + ' HTTP ' + resp.status + ' persistente após 2 retries: ' + (errObj.message || ''));
      }
      const waitMs = 5000 * (retryCount + 1);
      console.log('  ⏳ HTTP ' + resp.status + ' (tentativa ' + (retryCount + 1) + '/2), aguardando ' + (waitMs / 1000) + 's...');
      return sleep(waitMs).then(function () { return graphGet(endpoint, queryParams, retryCount + 1); });
    }

    throw new Error('Graph ' + endpoint + ' HTTP ' + resp.status + ': ' + (errObj.message || resp.body.slice(0, 200)));
  });
}

function graphPaginate(endpoint, queryParams, label) {
  const all = [];
  let pageCount = 0;

  function next(qParams, nextUrl) {
    pageCount += 1;
    if (pageCount > MAX_PAGES_PER_QUERY) {
      console.log('  ⚠️  Limite ' + MAX_PAGES_PER_QUERY + ' páginas em ' + label + ', parando');
      return Promise.resolve(all);
    }

    let promise;
    if (nextUrl) {
      const url = new URL(nextUrl);
      const params = new URLSearchParams(url.search || '');
      if (!params.get('access_token')) params.set('access_token', META_SYSTEM_USER_TOKEN);
      promise = httpsGet(url.pathname + '?' + params.toString()).then(function (resp) {
        if (resp.status !== 200) {
          const err = safeParse(resp.body);
          throw new Error('Paginação ' + label + ' HTTP ' + resp.status + ': ' + ((err.error && err.error.message) || ''));
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
// Geração de sub-janelas de datas
// ============================================================
function gerarJanelas(totalDias, chunkDias) {
  const janelas = [];
  const hoje = new Date();
  let offsetFim = 0;  // dias a partir de hoje

  while (offsetFim < totalDias) {
    const offsetInicio = Math.min(offsetFim + chunkDias, totalDias);
    const fim = new Date(hoje.getTime() - offsetFim * 86400000);
    const inicio = new Date(hoje.getTime() - (offsetInicio - 1) * 86400000);
    janelas.push({
      since: inicio.toISOString().slice(0, 10),
      until: fim.toISOString().slice(0, 10)
    });
    offsetFim = offsetInicio;
  }
  return janelas.reverse();  // mais antigo primeiro
}

// ============================================================
// coletarInsights com chunking automático
// ============================================================
async function coletarInsights(accountId, level, days, breakdowns, useMinimalFields) {
  // Decide se precisa chunkar: níveis granulares com janela > chunk
  const precisaChunkar = (level === 'adset' || level === 'ad') && days > CHUNK_DAYS_GRANULAR && !breakdowns;
  const janelas = precisaChunkar
    ? gerarJanelas(days, CHUNK_DAYS_GRANULAR)
    : [{
        since: new Date(Date.now() - days * 86400000).toISOString().slice(0, 10),
        until: new Date().toISOString().slice(0, 10)
      }];

  let fields;
  if (useMinimalFields) fields = INSIGHT_FIELDS_MINIMAL;
  else if (level === 'ad') fields = INSIGHT_FIELDS_BASE;
  else fields = INSIGHT_FIELDS_ACCOUNT;

  const bkLabel = breakdowns ? '[' + breakdowns + ']' : '';
  const chunkLabel = precisaChunkar ? ' [' + janelas.length + ' chunks de ' + CHUNK_DAYS_GRANULAR + 'd]' : '';
  console.log('  🔹 insights[' + level + bkLabel + '] act_' + accountId.slice(-4) + ' (' + days + 'd)' + chunkLabel);

  const todos = [];
  for (let i = 0; i < janelas.length; i++) {
    const j = janelas[i];
    const params = {
      fields: fields,
      level: level,
      time_range: JSON.stringify({ since: j.since, until: j.until }),
      limit: '500'
    };
    if (breakdowns) {
      params.breakdowns = breakdowns;
    } else {
      params.time_increment = '1';
    }

    const subLabel = 'insights[' + level + '] ' + j.since + '→' + j.until;
    if (precisaChunkar) console.log('    🗓️  Chunk ' + (i + 1) + '/' + janelas.length + ': ' + j.since + ' a ' + j.until);

    try {
      const rows = await graphPaginate('/act_' + accountId + '/insights', params, subLabel);
      todos.push.apply(todos, rows);
      if (precisaChunkar) await sleep(RATE_LIMIT_MS);
    } catch (err) {
      console.log('    ⚠️  Chunk falhou (' + j.since + '→' + j.until + '): ' + err.message);
      // Se falhou com campos completos, tentar com campos mínimos
      if (!useMinimalFields) {
        console.log('    🔄 Tentando com campos mínimos...');
        try {
          const paramsMin = Object.assign({}, params, { fields: INSIGHT_FIELDS_MINIMAL });
          const rows = await graphPaginate('/act_' + accountId + '/insights', paramsMin, subLabel + ' [min]');
          todos.push.apply(todos, rows);
        } catch (err2) {
          console.log('    ❌ Chunk falhou mesmo com campos mínimos: ' + err2.message);
          // Continua para próximo chunk
        }
      }
    }
  }

  console.log('    ✅ ' + level + ': ' + todos.length + ' linhas totais');
  return todos;
}

// ============================================================
// Metadados
// ============================================================
function coletarAccountMeta(accountId) {
  return graphGet('/act_' + accountId, {
    fields: 'id,account_id,name,currency,timezone_name,account_status,amount_spent,business_name,spend_cap'
  });
}

function coletarCampanhas(accountId) {
  console.log('  📋 Metadados de campanhas...');
  return graphPaginate('/act_' + accountId + '/campaigns', {
    fields: 'id,name,objective,status,effective_status,buying_type,special_ad_categories,created_time,start_time,stop_time,daily_budget,lifetime_budget',
    limit: '100'
  }, 'campanhas act_' + accountId.slice(-4)).catch(function (err) {
    console.log('    ⚠️  Falhou: ' + err.message);
    return [];
  });
}

function coletarAdsets(accountId) {
  console.log('  📋 Metadados de adsets...');
  return graphPaginate('/act_' + accountId + '/adsets', {
    fields: 'id,name,campaign_id,status,effective_status,optimization_goal,billing_event,bid_strategy,daily_budget,lifetime_budget,attribution_spec,created_time',
    limit: '100'
  }, 'adsets act_' + accountId.slice(-4)).catch(function (err) {
    console.log('    ⚠️  Falhou: ' + err.message);
    return [];
  });
}

function coletarAdsECriativos(accountId, onlyActive) {
  console.log('  🎨 Ads ' + (onlyActive ? 'ATIVOS' : 'TODOS') + ' + criativos...');
  let filtering = null;
  if (onlyActive) {
    filtering = JSON.stringify([{ field: 'effective_status', operator: 'IN', value: ['ACTIVE'] }]);
  }
  const params = {
    fields: 'id,name,status,effective_status,adset_id,campaign_id,created_time,updated_time,' +
            'creative{id,name,title,body,image_url,thumbnail_url,video_id,object_type,call_to_action_type,instagram_permalink_url,effective_object_story_id}',
    limit: '100'
  };
  if (filtering) params.filtering = filtering;
  return graphPaginate('/act_' + accountId + '/ads', params, 'ads act_' + accountId.slice(-4)).catch(function (err) {
    console.log('    ⚠️  Falhou: ' + err.message);
    return [];
  });
}

// ============================================================
// Enriquecimento (igual v2.0)
// ============================================================
function extractActionValue(list, types) {
  if (!list) return 0;
  let sum = 0;
  list.forEach(function (a) {
    if (types.indexOf(a.action_type) !== -1) sum += parseFloat(a.value) || 0;
  });
  return sum;
}

function enriquecerInsight(row) {
  const enriched = Object.assign({}, row);

  const purchaseAliases = ['purchase', 'omni_purchase', 'offsite_conversion.fb_pixel_purchase',
                           'onsite_web_purchase', 'onsite_web_app_purchase'];
  const addToCartAliases = ['add_to_cart', 'omni_add_to_cart', 'offsite_conversion.fb_pixel_add_to_cart'];
  const initCheckoutAliases = ['initiate_checkout', 'omni_initiated_checkout', 'offsite_conversion.fb_pixel_initiate_checkout'];
  const viewContentAliases = ['view_content', 'omni_view_content', 'offsite_conversion.fb_pixel_view_content'];

  enriched.purchases = extractActionValue(row.actions, purchaseAliases);
  enriched.purchaseValue = extractActionValue(row.action_values, purchaseAliases);
  enriched.addToCart = extractActionValue(row.actions, addToCartAliases);
  enriched.initiatedCheckout = extractActionValue(row.actions, initCheckoutAliases);
  enriched.viewContent = extractActionValue(row.actions, viewContentAliases);
  enriched.uniquePurchases = extractActionValue(row.unique_actions, purchaseAliases);

  enriched.spend = parseFloat(row.spend) || 0;
  enriched.impressions = parseInt(row.impressions) || 0;
  enriched.clicks = parseInt(row.clicks) || 0;
  enriched.reach = parseInt(row.reach) || 0;
  enriched.frequency = parseFloat(row.frequency) || 0;
  enriched.inlineLinkClicks = parseInt(row.inline_link_clicks) || 0;
  enriched.outboundClicks = row.outbound_clicks ?
    (row.outbound_clicks.reduce(function (s, x) { return s + (parseFloat(x.value) || 0); }, 0)) : 0;
  enriched.uniqueClicks = parseInt(row.unique_clicks) || 0;
  enriched.socialSpend = parseFloat(row.social_spend) || 0;

  enriched.videoPlays = extractActionValue(row.video_play_actions, ['video_view']);
  enriched.videoThruplay = extractActionValue(row.video_thruplay_watched_actions, ['video_view']);
  enriched.videoP25 = extractActionValue(row.video_p25_watched_actions, ['video_view']);
  enriched.videoP50 = extractActionValue(row.video_p50_watched_actions, ['video_view']);
  enriched.videoP75 = extractActionValue(row.video_p75_watched_actions, ['video_view']);
  enriched.videoP100 = extractActionValue(row.video_p100_watched_actions, ['video_view']);

  if (row.quality_ranking) enriched.qualityRanking = row.quality_ranking;
  if (row.engagement_rate_ranking) enriched.engagementRateRanking = row.engagement_rate_ranking;
  if (row.conversion_rate_ranking) enriched.conversionRateRanking = row.conversion_rate_ranking;

  enriched.roas = enriched.spend > 0 ? enriched.purchaseValue / enriched.spend : 0;
  enriched.cpa = enriched.purchases > 0 ? enriched.spend / enriched.purchases : 0;
  enriched.linkCtr = enriched.impressions > 0 ? (enriched.inlineLinkClicks / enriched.impressions) * 100 : 0;
  enriched.videoViewRate = enriched.impressions > 0 ? (enriched.videoPlays / enriched.impressions) * 100 : 0;
  enriched.thruplayRate = enriched.videoPlays > 0 ? (enriched.videoThruplay / enriched.videoPlays) * 100 : 0;

  return enriched;
}

// ============================================================
// Cache criativos
// ============================================================
function carregarCacheCriativos() {
  if (fs.existsSync(CREATIVES_CACHE_FILE)) {
    try {
      const data = JSON.parse(fs.readFileSync(CREATIVES_CACHE_FILE, 'utf-8'));
      console.log('📥 Cache criativos: ' + Object.keys(data).length + ' ads');
      return data;
    } catch (e) { return {}; }
  }
  return {};
}

function salvarCacheCriativos(cache) {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  fs.writeFileSync(CREATIVES_CACHE_FILE, JSON.stringify(cache));
}

// ============================================================
// Stats (igual v2.0)
// ============================================================
function computeStats(accountInsights, campaignInsights, adInsights) {
  const hoje = new Date();
  const d7 = new Date(hoje.getTime() - 7 * 86400000);
  const d30 = new Date(hoje.getTime() - 30 * 86400000);
  const d90 = new Date(hoje.getTime() - 90 * 86400000);

  function aggregate(rows, desde) {
    const filtered = desde ? rows.filter(function (r) {
      return new Date(r.date_start || 0) >= desde;
    }) : rows;
    let spend = 0, purchases = 0, purchaseValue = 0, impressions = 0, clicks = 0,
        linkClicks = 0, addToCart = 0, initiatedCheckout = 0;
    filtered.forEach(function (r) {
      spend += r.spend || 0;
      purchases += r.purchases || 0;
      purchaseValue += r.purchaseValue || 0;
      impressions += r.impressions || 0;
      clicks += r.clicks || 0;
      linkClicks += r.inlineLinkClicks || 0;
      addToCart += r.addToCart || 0;
      initiatedCheckout += r.initiatedCheckout || 0;
    });
    return {
      spend: spend, purchases: purchases, purchaseValue: purchaseValue,
      impressions: impressions, clicks: clicks, linkClicks: linkClicks,
      addToCart: addToCart, initiatedCheckout: initiatedCheckout,
      roas: spend > 0 ? purchaseValue / spend : 0,
      cpa: purchases > 0 ? spend / purchases : 0,
      ctr: impressions > 0 ? (clicks / impressions) * 100 : 0,
      linkCtr: impressions > 0 ? (linkClicks / impressions) * 100 : 0,
      convRate: linkClicks > 0 ? (purchases / linkClicks) * 100 : 0
    };
  }

  const byMonth = {};
  accountInsights.forEach(function (r) {
    const m = (r.date_start || '').slice(0, 7);
    if (!m) return;
    if (!byMonth[m]) byMonth[m] = { spend: 0, purchases: 0, purchaseValue: 0 };
    byMonth[m].spend += r.spend || 0;
    byMonth[m].purchases += r.purchases || 0;
    byMonth[m].purchaseValue += r.purchaseValue || 0;
  });
  Object.keys(byMonth).forEach(function (m) {
    byMonth[m].roas = byMonth[m].spend > 0 ? byMonth[m].purchaseValue / byMonth[m].spend : 0;
  });

  const byCampaign = {};
  campaignInsights.forEach(function (r) {
    const id = r.campaign_id || 'unknown';
    if (!byCampaign[id]) {
      byCampaign[id] = {
        campaign_id: id, campaign_name: r.campaign_name || '(sem nome)',
        account_id: r.account_id, spend: 0, purchases: 0, purchaseValue: 0,
        impressions: 0, clicks: 0
      };
    }
    byCampaign[id].spend += r.spend || 0;
    byCampaign[id].purchases += r.purchases || 0;
    byCampaign[id].purchaseValue += r.purchaseValue || 0;
    byCampaign[id].impressions += r.impressions || 0;
    byCampaign[id].clicks += r.clicks || 0;
  });
  const campArr = Object.values(byCampaign).map(function (c) {
    c.roas = c.spend > 0 ? c.purchaseValue / c.spend : 0;
    c.cpa = c.purchases > 0 ? c.spend / c.purchases : 0;
    return c;
  });

  const byAd = {};
  adInsights.forEach(function (r) {
    const id = r.ad_id || 'unknown';
    if (!byAd[id]) {
      byAd[id] = {
        ad_id: id, ad_name: r.ad_name || '(sem nome)',
        campaign_id: r.campaign_id, adset_id: r.adset_id, account_id: r.account_id,
        spend: 0, purchases: 0, purchaseValue: 0, impressions: 0,
        videoPlays: 0, videoThruplay: 0,
        qualityRanking: r.qualityRanking,
        engagementRateRanking: r.engagementRateRanking,
        conversionRateRanking: r.conversionRateRanking
      };
    }
    byAd[id].spend += r.spend || 0;
    byAd[id].purchases += r.purchases || 0;
    byAd[id].purchaseValue += r.purchaseValue || 0;
    byAd[id].impressions += r.impressions || 0;
    byAd[id].videoPlays += r.videoPlays || 0;
    byAd[id].videoThruplay += r.videoThruplay || 0;
  });
  const adArr = Object.values(byAd).map(function (a) {
    a.roas = a.spend > 0 ? a.purchaseValue / a.spend : 0;
    a.cpa = a.purchases > 0 ? a.spend / a.purchases : 0;
    a.thruplayRate = a.videoPlays > 0 ? (a.videoThruplay / a.videoPlays) * 100 : 0;
    return a;
  });

  return {
    consolidated: {
      '7d': aggregate(accountInsights, d7),
      '30d': aggregate(accountInsights, d30),
      '90d': aggregate(accountInsights, d90),
      total: aggregate(accountInsights, null)
    },
    byMonth: byMonth,
    totalCampaigns: campArr.length,
    topCampaignsBySpend: campArr.slice().sort(function (a, b) { return b.spend - a.spend; }).slice(0, 20),
    topCampaignsByRoas: campArr.filter(function (c) { return c.spend >= 100; })
      .sort(function (a, b) { return b.roas - a.roas; }).slice(0, 20),
    totalAds: adArr.length,
    topAdsBySpend: adArr.slice().sort(function (a, b) { return b.spend - a.spend; }).slice(0, 30),
    topAdsByRoas: adArr.filter(function (a) { return a.spend >= 50; })
      .sort(function (a, b) { return b.roas - a.roas; }).slice(0, 30)
  };
}

// ============================================================
// MAIN
// ============================================================
async function main() {
  console.log('🚀 Suplemind Meta Ads Collector v2.1');
  console.log('   Modo: ' + MODE);
  console.log('   Timestamp: ' + new Date().toISOString());
  console.log('   Graph API: ' + GRAPH_API_VERSION);
  console.log('   Chunk size granular: ' + CHUNK_DAYS_GRANULAR + 'd');
  console.log('');

  if (!META_APP_ID || !META_APP_SECRET || !META_SYSTEM_USER_TOKEN || !META_AD_ACCOUNT_IDS) {
    throw new Error('Secrets Meta ausentes');
  }

  const accountIds = META_AD_ACCOUNT_IDS.split(',').map(function (s) { return s.trim(); }).filter(Boolean);
  console.log('📋 Ad Accounts: ' + accountIds.length);

  const daysMain = MODE === 'incremental' ? DAYS_INCREMENTAL : DAYS_FULL;
  const daysAd = MODE === 'incremental' ? DAYS_INCREMENTAL : DAYS_AD_LEVEL;
  const daysBreakdown = MODE === 'incremental' ? DAYS_INCREMENTAL : DAYS_BREAKDOWN;

  const creativesCache = carregarCacheCriativos();

  const output = {
    meta: {
      version: '2.1',
      collectedAt: new Date().toISOString(),
      mode: MODE,
      graphApiVersion: GRAPH_API_VERSION,
      windows: { main: daysMain, ad: daysAd, breakdown: daysBreakdown }
    },
    accounts: {},
    insights: { account: [], campaign: [], adset: [], ad: [] },
    breakdowns: { byAgeGender: [], byPublisherPlatform: [], byPlatformPosition: [] },
    campaigns: [], adsets: [], ads: [], creatives: {},
    errors: []  // NOVO: registra o que falhou
  };

  for (const accId of accountIds) {
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    console.log('📊 act_' + accId + ' (' + (ACCOUNT_ALIASES[accId] || 'n/a') + ')');
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');

    try {
      const meta = await coletarAccountMeta(accId);
      output.accounts['act_' + accId] = {
        id: meta.id, account_id: meta.account_id, name: meta.name,
        business_name: meta.business_name, currency: meta.currency,
        timezone: meta.timezone_name, account_status: meta.account_status,
        amount_spent_total: meta.amount_spent, spend_cap: meta.spend_cap,
        alias: ACCOUNT_ALIASES[accId] || ('Conta ' + accId.slice(-4))
      };
      console.log('  ✅ ' + meta.name);
    } catch (err) {
      console.log('  ❌ Metadata falhou: ' + err.message);
      output.errors.push({ account: accId, step: 'metadata', error: err.message });
      continue;
    }
    await sleep(RATE_LIMIT_MS);

    // Insights 4 níveis — cada um em try/catch independente
    const levels = [
      { name: 'account', days: daysMain },
      { name: 'campaign', days: daysMain },
      { name: 'adset', days: daysMain },
      { name: 'ad', days: daysAd }
    ];
    for (const lv of levels) {
      try {
        const raw = await coletarInsights(accId, lv.name, lv.days, null, false);
        const enriched = raw.map(enriquecerInsight).map(function (r) { r.account_id = accId; return r; });
        output.insights[lv.name].push.apply(output.insights[lv.name], enriched);
      } catch (err) {
        console.log('  ❌ insights[' + lv.name + '] falhou: ' + err.message);
        output.errors.push({ account: accId, step: 'insights_' + lv.name, error: err.message });
      }
      await sleep(RATE_LIMIT_MS);
    }

    // Breakdowns (só full)
    if (MODE === 'full') {
      console.log('  🎯 Breakdowns (30d)...');
      const bkConfigs = [
        { name: 'byAgeGender', bk: 'age,gender' },
        { name: 'byPublisherPlatform', bk: 'publisher_platform' },
        { name: 'byPlatformPosition', bk: 'publisher_platform,platform_position' }
      ];
      for (const bc of bkConfigs) {
        try {
          const raw = await coletarInsights(accId, 'account', daysBreakdown, bc.bk, false);
          const enriched = raw.map(enriquecerInsight).map(function (r) { r.account_id = accId; return r; });
          output.breakdowns[bc.name].push.apply(output.breakdowns[bc.name], enriched);
          console.log('    ✅ ' + bc.name + ': ' + enriched.length + ' linhas');
        } catch (err) {
          console.log('    ⚠️  ' + bc.name + ': ' + err.message);
          output.errors.push({ account: accId, step: 'bk_' + bc.name, error: err.message });
        }
        await sleep(RATE_LIMIT_MS);
      }
    }

    // Metadados
    try {
      const campaigns = await coletarCampanhas(accId);
      campaigns.forEach(function (c) { c.account_id = accId; });
      output.campaigns.push.apply(output.campaigns, campaigns);
      console.log('  ✅ Campanhas: ' + campaigns.length);
    } catch (err) {
      output.errors.push({ account: accId, step: 'campaigns', error: err.message });
    }
    await sleep(RATE_LIMIT_MS);

    try {
      const adsets = await coletarAdsets(accId);
      adsets.forEach(function (a) { a.account_id = accId; });
      output.adsets.push.apply(output.adsets, adsets);
      console.log('  ✅ Adsets: ' + adsets.length);
    } catch (err) {
      output.errors.push({ account: accId, step: 'adsets', error: err.message });
    }
    await sleep(RATE_LIMIT_MS);

    // Criativos
    if (MODE === 'full') {
      try {
        const ads = await coletarAdsECriativos(accId, true);
        ads.forEach(function (a) {
          a.account_id = accId;
          output.ads.push(a);
          if (a.creative) {
            output.creatives[a.id] = {
              ad_id: a.id, creative_id: a.creative.id,
              name: a.creative.name || a.name,
              title: a.creative.title, body: a.creative.body,
              image_url: a.creative.image_url, thumbnail_url: a.creative.thumbnail_url,
              video_id: a.creative.video_id, object_type: a.creative.object_type,
              call_to_action_type: a.creative.call_to_action_type,
              instagram_permalink_url: a.creative.instagram_permalink_url,
              collectedAt: new Date().toISOString()
            };
            creativesCache[a.id] = output.creatives[a.id];
          }
        });
        console.log('  ✅ Ads ativos + criativos: ' + ads.length);
      } catch (err) {
        output.errors.push({ account: accId, step: 'ads_criativos', error: err.message });
      }
      await sleep(RATE_LIMIT_MS);
    } else {
      Object.assign(output.creatives, creativesCache);
      console.log('  📥 Criativos do cache: ' + Object.keys(creativesCache).length);
    }
  }

  if (Object.keys(output.creatives).length > 0) salvarCacheCriativos(output.creatives);

  console.log('');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log('📊 Calculando estatísticas...');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');

  output.stats = computeStats(output.insights.account, output.insights.campaign, output.insights.ad);
  output.meta.counts = {
    accounts: Object.keys(output.accounts).length,
    accountInsights: output.insights.account.length,
    campaignInsights: output.insights.campaign.length,
    adsetInsights: output.insights.adset.length,
    adInsights: output.insights.ad.length,
    campaigns: output.campaigns.length,
    adsets: output.adsets.length,
    ads: output.ads.length,
    creatives: Object.keys(output.creatives).length,
    breakdownAgeGender: output.breakdowns.byAgeGender.length,
    breakdownPlatform: output.breakdowns.byPublisherPlatform.length,
    breakdownPosition: output.breakdowns.byPlatformPosition.length,
    errors: output.errors.length
  };

  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  fs.writeFileSync(DATA_FILE, JSON.stringify(output, null, 2));
  const sizeKB = (fs.statSync(DATA_FILE).size / 1024).toFixed(1);

  console.log('');
  console.log('✅ Coleta Meta concluída! Arquivo: ' + sizeKB + ' KB');

  if (output.errors.length > 0) {
    console.log('');
    console.log('⚠️  ERROS REGISTRADOS (' + output.errors.length + '):');
    output.errors.forEach(function (e) {
      console.log('   [' + e.step + '] act_' + e.account.slice(-4) + ': ' + e.error.slice(0, 120));
    });
  }

  console.log('');
  console.log('📈 RESUMO CONSOLIDADO:');
  ['7d', '30d', '90d', 'total'].forEach(function (p) {
    const s = output.stats.consolidated[p];
    console.log('   ' + p + ':  Spend R$ ' + s.spend.toFixed(2) +
                ' | Purch ' + s.purchases +
                ' | Rev R$ ' + s.purchaseValue.toFixed(2) +
                ' | ROAS ' + s.roas.toFixed(2) + 'x' +
                ' | CPA R$ ' + s.cpa.toFixed(2));
  });

  console.log('');
  console.log('📊 Por mês (últimos 6):');
  const meses = Object.keys(output.stats.byMonth).sort().slice(-6);
  meses.forEach(function (m) {
    const d = output.stats.byMonth[m];
    console.log('   ' + m + ': R$ ' + d.spend.toFixed(2) +
                ' | Rev R$ ' + d.purchaseValue.toFixed(2) +
                ' | ROAS ' + d.roas.toFixed(2) + 'x');
  });

  console.log('');
  console.log('🎯 Top 3 campanhas por spend:');
  output.stats.topCampaignsBySpend.slice(0, 3).forEach(function (c, i) {
    console.log('   ' + (i + 1) + '. ' + c.campaign_name.slice(0, 60) +
                ' — R$ ' + c.spend.toFixed(2) + ' (ROAS ' + c.roas.toFixed(2) + 'x)');
  });

  console.log('');
  console.log('🎨 Criativos: ' + Object.keys(output.creatives).length);
  console.log('🎬 Ads com vídeo: ' + output.ads.filter(function (a) {
    return a.creative && a.creative.video_id;
  }).length);
}

main().catch(function (err) {
  console.error('❌ ERRO FATAL: ' + err.message);
  console.error(err.stack);
  process.exit(1);
});
