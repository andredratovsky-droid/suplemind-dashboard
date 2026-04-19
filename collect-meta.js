// collect-meta.js v2.0 — Coletor Meta Ads completo (Graph API v23.0)
//
// Expansões vs v1.0:
//   - Campos adicionais: unique_clicks/ctr, inline/outbound clicks, video metrics,
//     quality rankings, objective, cost_per_action_type
//   - Breakdowns: age+gender, publisher_platform, platform_position
//   - Coleta de criativos (endpoint /ads com fields=creative{})
//   - Endpoint /campaigns e /adsets pra metadados (objective, status)
//
// Modos (via env COLLECT_MODE):
//   - "full"        : 180 dias todos os níveis, 30 dias ad, criativos de ads ativos
//   - "incremental" : 3 dias em todos os níveis, não refaz criativos

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

const DAYS_FULL = 180;
const DAYS_AD_LEVEL = 30;
const DAYS_INCREMENTAL = 3;
const DAYS_BREAKDOWN = 30;  // Breakdowns custam mais, manter em 30d

const ACCOUNT_ALIASES = {
  '929553552297011': 'Principal',
  '1169095775160589': 'Secundária'
};

// ============================================================
// CAMPOS DE INSIGHTS (muito expandidos vs v1.0)
// ============================================================
const INSIGHT_FIELDS_BASE = [
  // Métricas básicas
  'spend',
  'impressions',
  'clicks',
  'ctr',
  'cpc',
  'cpm',
  'cpp',
  'reach',
  'frequency',

  // Cliques granulares
  'inline_link_clicks',
  'outbound_clicks',
  'unique_clicks',
  'unique_ctr',
  'unique_inline_link_clicks',
  'unique_link_clicks_ctr',

  // Custo por tipo
  'cost_per_inline_link_click',
  'cost_per_outbound_click',
  'cost_per_unique_click',
  'cost_per_unique_inline_link_click',

  // Ações e valores (onde moram purchases, add_to_cart, etc)
  'actions',
  'action_values',
  'unique_actions',
  'cost_per_action_type',
  'cost_per_unique_action_type',
  'conversions',
  'conversion_values',

  // Engajamento
  'inline_post_engagement',
  'social_spend',

  // Vídeo
  'video_play_actions',
  'video_thruplay_watched_actions',
  'video_p25_watched_actions',
  'video_p50_watched_actions',
  'video_p75_watched_actions',
  'video_p100_watched_actions',
  'video_avg_time_watched_actions',
  'cost_per_thruplay',

  // Qualidade (só disponível nível ad)
  'quality_ranking',
  'engagement_rate_ranking',
  'conversion_rate_ranking'
].join(',');

// Para nível campaign/adset/account, remover campos só disponíveis em ad
const INSIGHT_FIELDS_ACCOUNT = INSIGHT_FIELDS_BASE
  .split(',')
  .filter(function (f) { return !['quality_ranking', 'engagement_rate_ranking', 'conversion_rate_ranking'].includes(f); })
  .join(',');

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
    req.end();
  });
}

function sleep(ms) { return new Promise(function (r) { setTimeout(r, ms); }); }

function safeParse(body) {
  try { return JSON.parse(body); } catch (e) { return { _raw: body }; }
}

function graphGet(endpoint, queryParams) {
  const qs = new URLSearchParams(queryParams || {});
  qs.set('access_token', META_SYSTEM_USER_TOKEN);
  const fullPath = '/' + GRAPH_API_VERSION + endpoint + '?' + qs.toString();

  return httpsGet(fullPath).then(function (resp) {
    if (resp.status === 200) return safeParse(resp.body);
    const err = safeParse(resp.body);
    const errObj = err.error || {};
    if (errObj.code === 17 || errObj.code === 613 || errObj.code === 4 || resp.status === 429) {
      console.log('  ⏳ Rate limit, aguardando 60s...');
      return sleep(60000).then(function () { return graphGet(endpoint, queryParams); });
    }
    // Alguns erros de breakdown em certos níveis — retornar vazio em vez de quebrar
    if (errObj.code === 100 && errObj.message && errObj.message.indexOf('breakdown') !== -1) {
      console.log('  ⚠️  Breakdown não suportado neste nível, pulando: ' + errObj.message);
      return { data: [] };
    }
    throw new Error('Graph ' + endpoint + ' HTTP ' + resp.status + ': ' + (errObj.message || resp.body.slice(0, 200)));
  });
}

function graphPaginate(endpoint, queryParams, label) {
  const all = [];
  let pageCount = 0;

  function next(qParams, nextUrl) {
    pageCount += 1;
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
// Coletas principais
// ============================================================
function coletarAccountMeta(accountId) {
  return graphGet('/act_' + accountId, {
    fields: 'id,account_id,name,currency,timezone_name,account_status,amount_spent,business_name,spend_cap'
  });
}

// Coleta insights com possível breakdown
function coletarInsights(accountId, level, days, breakdowns) {
  const since = new Date(Date.now() - days * 86400000).toISOString().slice(0, 10);
  const until = new Date().toISOString().slice(0, 10);

  // Fields: usar FIELDS_BASE só em nível ad (para quality_ranking etc)
  const fields = level === 'ad' ? INSIGHT_FIELDS_BASE : INSIGHT_FIELDS_ACCOUNT;

  const params = {
    fields: fields,
    level: level,
    time_range: JSON.stringify({ since: since, until: until }),
    limit: '500'
  };

  // Breakdowns não combinam bem com time_increment
  // Se tem breakdown, não quebra por dia (cada dimensão multiplica linhas)
  if (breakdowns) {
    params.breakdowns = breakdowns;
  } else {
    params.time_increment = '1';
  }

  const bkLabel = breakdowns ? '[' + breakdowns + ']' : '';
  const label = 'insights[' + level + bkLabel + '] act_' + accountId.slice(-4) + ' (' + days + 'd)';
  console.log('  🔹 ' + label);

  return graphPaginate('/act_' + accountId + '/insights', params, label);
}

// Metadados de campanhas (objective, status, orçamento)
function coletarCampanhas(accountId) {
  console.log('  📋 Coletando metadados de campanhas...');
  return graphPaginate('/act_' + accountId + '/campaigns', {
    fields: 'id,name,objective,status,effective_status,buying_type,special_ad_categories,created_time,start_time,stop_time,daily_budget,lifetime_budget',
    limit: '100'
  }, 'campanhas act_' + accountId.slice(-4));
}

// Metadados de adsets (optimization_goal, targeting resumido)
function coletarAdsets(accountId) {
  console.log('  📋 Coletando metadados de adsets...');
  return graphPaginate('/act_' + accountId + '/adsets', {
    fields: 'id,name,campaign_id,status,effective_status,optimization_goal,billing_event,bid_strategy,daily_budget,lifetime_budget,attribution_spec,created_time',
    limit: '100'
  }, 'adsets act_' + accountId.slice(-4));
}

// Coleta ads ativos + seus criativos (batelada grande)
function coletarAdsECriativos(accountId, onlyActive) {
  console.log('  🎨 Coletando ads ' + (onlyActive ? 'ATIVOS' : 'TODOS') + ' com criativos...');

  // Filtrar só ads ativos quando for full scan inicial (economiza muito)
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

  return graphPaginate('/act_' + accountId + '/ads', params, 'ads act_' + accountId.slice(-4));
}

// ============================================================
// Enriquecimento de linhas de insights
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

  // Purchases (múltiplos aliases — pixel, CAPI, offsite)
  const purchaseAliases = [
    'purchase',
    'omni_purchase',
    'offsite_conversion.fb_pixel_purchase',
    'onsite_web_purchase',
    'onsite_web_app_purchase'
  ];
  const addToCartAliases = [
    'add_to_cart',
    'omni_add_to_cart',
    'offsite_conversion.fb_pixel_add_to_cart'
  ];
  const initCheckoutAliases = [
    'initiate_checkout',
    'omni_initiated_checkout',
    'offsite_conversion.fb_pixel_initiate_checkout'
  ];
  const viewContentAliases = [
    'view_content',
    'omni_view_content',
    'offsite_conversion.fb_pixel_view_content'
  ];

  enriched.purchases = extractActionValue(row.actions, purchaseAliases);
  enriched.purchaseValue = extractActionValue(row.action_values, purchaseAliases);
  enriched.addToCart = extractActionValue(row.actions, addToCartAliases);
  enriched.initiatedCheckout = extractActionValue(row.actions, initCheckoutAliases);
  enriched.viewContent = extractActionValue(row.actions, viewContentAliases);

  enriched.uniquePurchases = extractActionValue(row.unique_actions, purchaseAliases);

  // Métricas numéricas
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

  // Vídeo — campos vêm como array [{action_type: "video_view", value: "123"}]
  enriched.videoPlays = extractActionValue(row.video_play_actions, ['video_view']);
  enriched.videoThruplay = extractActionValue(row.video_thruplay_watched_actions, ['video_view']);
  enriched.videoP25 = extractActionValue(row.video_p25_watched_actions, ['video_view']);
  enriched.videoP50 = extractActionValue(row.video_p50_watched_actions, ['video_view']);
  enriched.videoP75 = extractActionValue(row.video_p75_watched_actions, ['video_view']);
  enriched.videoP100 = extractActionValue(row.video_p100_watched_actions, ['video_view']);

  // Rankings (só em nível ad)
  if (row.quality_ranking) enriched.qualityRanking = row.quality_ranking;
  if (row.engagement_rate_ranking) enriched.engagementRateRanking = row.engagement_rate_ranking;
  if (row.conversion_rate_ranking) enriched.conversionRateRanking = row.conversion_rate_ranking;

  // Calculados
  enriched.roas = enriched.spend > 0 ? enriched.purchaseValue / enriched.spend : 0;
  enriched.cpa = enriched.purchases > 0 ? enriched.spend / enriched.purchases : 0;
  enriched.calcCtr = enriched.impressions > 0 ? (enriched.clicks / enriched.impressions) * 100 : 0;
  enriched.linkCtr = enriched.impressions > 0 ? (enriched.inlineLinkClicks / enriched.impressions) * 100 : 0;
  enriched.videoViewRate = enriched.impressions > 0 ? (enriched.videoPlays / enriched.impressions) * 100 : 0;
  enriched.thruplayRate = enriched.videoPlays > 0 ? (enriched.videoThruplay / enriched.videoPlays) * 100 : 0;

  return enriched;
}

// ============================================================
// Cache de criativos (evita re-baixar toda run)
// ============================================================
function carregarCacheCriativos() {
  if (fs.existsSync(CREATIVES_CACHE_FILE)) {
    try {
      const data = JSON.parse(fs.readFileSync(CREATIVES_CACHE_FILE, 'utf-8'));
      console.log('📥 Cache de criativos carregado: ' + Object.keys(data).length + ' ads');
      return data;
    } catch (e) {
      console.log('⚠️  Cache criativos corrompido');
      return {};
    }
  }
  return {};
}

function salvarCacheCriativos(cache) {
  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  fs.writeFileSync(CREATIVES_CACHE_FILE, JSON.stringify(cache));
}

// ============================================================
// Stats consolidadas
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

  // Por mês (para gráfico evolutivo)
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
    const d = byMonth[m];
    d.roas = d.spend > 0 ? d.purchaseValue / d.spend : 0;
  });

  // Top campanhas
  const byCampaign = {};
  campaignInsights.forEach(function (r) {
    const id = r.campaign_id || 'unknown';
    if (!byCampaign[id]) {
      byCampaign[id] = {
        campaign_id: id,
        campaign_name: r.campaign_name || '(sem nome)',
        account_id: r.account_id,
        spend: 0, purchases: 0, purchaseValue: 0, impressions: 0, clicks: 0
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

  // Top ads (últimos 30d)
  const byAd = {};
  adInsights.forEach(function (r) {
    const id = r.ad_id || 'unknown';
    if (!byAd[id]) {
      byAd[id] = {
        ad_id: id,
        ad_name: r.ad_name || '(sem nome)',
        campaign_id: r.campaign_id,
        adset_id: r.adset_id,
        account_id: r.account_id,
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
  console.log('🚀 Suplemind Meta Ads Collector v2.0');
  console.log('   Modo: ' + MODE);
  console.log('   Timestamp: ' + new Date().toISOString());
  console.log('   Graph API: ' + GRAPH_API_VERSION);
  console.log('');

  if (!META_APP_ID || !META_APP_SECRET || !META_SYSTEM_USER_TOKEN || !META_AD_ACCOUNT_IDS) {
    throw new Error('Secrets Meta ausentes');
  }

  const accountIds = META_AD_ACCOUNT_IDS.split(',').map(function (s) { return s.trim(); }).filter(Boolean);
  console.log('📋 Ad Accounts: ' + accountIds.length);
  console.log('');

  const daysMain = MODE === 'incremental' ? DAYS_INCREMENTAL : DAYS_FULL;
  const daysAd = MODE === 'incremental' ? DAYS_INCREMENTAL : DAYS_AD_LEVEL;
  const daysBreakdown = MODE === 'incremental' ? DAYS_INCREMENTAL : DAYS_BREAKDOWN;

  const creativesCache = carregarCacheCriativos();

  const output = {
    meta: {
      version: '2.0',
      collectedAt: new Date().toISOString(),
      mode: MODE,
      graphApiVersion: GRAPH_API_VERSION,
      windows: { main: daysMain, ad: daysAd, breakdown: daysBreakdown }
    },
    accounts: {},
    insights: {
      account: [],
      campaign: [],
      adset: [],
      ad: []
    },
    breakdowns: {
      byAgeGender: [],
      byPublisherPlatform: [],
      byPlatformPosition: []
    },
    campaigns: [],
    adsets: [],
    ads: [],
    creatives: {}
  };

  // Loop por conta
  for (const accId of accountIds) {
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    console.log('📊 act_' + accId + ' (' + (ACCOUNT_ALIASES[accId] || 'n/a') + ')');
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');

    // Metadata
    const meta = await coletarAccountMeta(accId);
    output.accounts['act_' + accId] = {
      id: meta.id, account_id: meta.account_id, name: meta.name,
      business_name: meta.business_name, currency: meta.currency,
      timezone: meta.timezone_name, account_status: meta.account_status,
      amount_spent_total: meta.amount_spent, spend_cap: meta.spend_cap,
      alias: ACCOUNT_ALIASES[accId] || ('Conta ' + accId.slice(-4))
    };
    console.log('  ✅ ' + meta.name);
    await sleep(RATE_LIMIT_MS);

    // Insights 4 níveis
    const levels = [
      { name: 'account', days: daysMain },
      { name: 'campaign', days: daysMain },
      { name: 'adset', days: daysMain },
      { name: 'ad', days: daysAd }
    ];
    for (const lv of levels) {
      const raw = await coletarInsights(accId, lv.name, lv.days, null);
      const enriched = raw.map(enriquecerInsight).map(function (r) { r.account_id = accId; return r; });
      output.insights[lv.name].push.apply(output.insights[lv.name], enriched);
      console.log('    ✅ ' + lv.name + ': ' + enriched.length + ' linhas');
      await sleep(RATE_LIMIT_MS);
    }

    // Breakdowns (só em full mode, e só 30d)
    if (MODE === 'full') {
      console.log('  🎯 Coletando breakdowns (30d)...');
      const breakdownConfigs = [
        { name: 'byAgeGender', bk: 'age,gender' },
        { name: 'byPublisherPlatform', bk: 'publisher_platform' },
        { name: 'byPlatformPosition', bk: 'publisher_platform,platform_position' }
      ];
      for (const bc of breakdownConfigs) {
        const raw = await coletarInsights(accId, 'account', daysBreakdown, bc.bk);
        const enriched = raw.map(enriquecerInsight).map(function (r) { r.account_id = accId; return r; });
        output.breakdowns[bc.name].push.apply(output.breakdowns[bc.name], enriched);
        console.log('    ✅ ' + bc.name + ': ' + enriched.length + ' linhas');
        await sleep(RATE_LIMIT_MS);
      }
    }

    // Metadados de campanhas e adsets
    const campaigns = await coletarCampanhas(accId);
    campaigns.forEach(function (c) { c.account_id = accId; });
    output.campaigns.push.apply(output.campaigns, campaigns);
    console.log('  ✅ Campanhas: ' + campaigns.length);
    await sleep(RATE_LIMIT_MS);

    const adsets = await coletarAdsets(accId);
    adsets.forEach(function (a) { a.account_id = accId; });
    output.adsets.push.apply(output.adsets, adsets);
    console.log('  ✅ Adsets: ' + adsets.length);
    await sleep(RATE_LIMIT_MS);

    // Criativos (em full mode, só ads ativos; em incremental, não refaz)
    if (MODE === 'full') {
      const ads = await coletarAdsECriativos(accId, true);
      ads.forEach(function (a) {
        a.account_id = accId;
        output.ads.push(a);
        if (a.creative) {
          output.creatives[a.id] = {
            ad_id: a.id,
            creative_id: a.creative.id,
            name: a.creative.name || a.name,
            title: a.creative.title,
            body: a.creative.body,
            image_url: a.creative.image_url,
            thumbnail_url: a.creative.thumbnail_url,
            video_id: a.creative.video_id,
            object_type: a.creative.object_type,
            call_to_action_type: a.creative.call_to_action_type,
            instagram_permalink_url: a.creative.instagram_permalink_url,
            collectedAt: new Date().toISOString()
          };
          creativesCache[a.id] = output.creatives[a.id];
        }
      });
      console.log('  ✅ Ads ativos + criativos: ' + ads.length);
      await sleep(RATE_LIMIT_MS);
    } else {
      // Em incremental, reutilizar cache
      Object.assign(output.creatives, creativesCache);
      console.log('  📥 Criativos reutilizados do cache: ' + Object.keys(creativesCache).length);
    }
  }

  // Salvar cache de criativos
  if (Object.keys(output.creatives).length > 0) salvarCacheCriativos(output.creatives);

  // Stats
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
    breakdownPosition: output.breakdowns.byPlatformPosition.length
  };

  if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
  fs.writeFileSync(DATA_FILE, JSON.stringify(output, null, 2));
  const sizeKB = (fs.statSync(DATA_FILE).size / 1024).toFixed(1);

  console.log('');
  console.log('✅ Coleta Meta concluída! Arquivo: ' + sizeKB + ' KB');
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
  console.log('🎨 Criativos coletados: ' + Object.keys(output.creatives).length);
  console.log('🎬 Ads com vídeo: ' + output.ads.filter(function (a) {
    return a.creative && a.creative.video_id;
  }).length);
}

main().catch(function (err) {
  console.error('❌ ERRO: ' + err.message);
  console.error(err.stack);
  process.exit(1);
});
