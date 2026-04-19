// collect-meta.js v2.2 — Versão SLIM (target: <15 MB em meta.json)
//
// Mudanças vs v2.1:
//   - Campos brutos (actions, action_values, unique_actions, etc) REMOVIDOS após extração
//   - Nomes curtos nos insights (sp=spend, pv=purchaseValue, vp=videoPlays, etc)
//   - JSON compacto (sem pretty-print)
//   - Corrigido bug: campaign_name agora é coletado corretamente
//   - Corrigido bug: ad_name agora é coletado corretamente
//   - Campos de ranking só incluídos se presentes (economiza nulls)
//   - Criativos: só URLs/IDs essenciais (remove body texts muito longos)

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
const MAX_PAGES_PER_QUERY = 30;

const DAYS_FULL = 180;
const DAYS_AD_LEVEL = 30;
const DAYS_INCREMENTAL = 3;
const DAYS_BREAKDOWN = 30;
const CHUNK_DAYS_GRANULAR = 30;

const ACCOUNT_ALIASES = {
  '929553552297011': 'Principal',
  '1169095775160589': 'Secundária'
};

// ============================================================
// CAMPOS
// ============================================================
// Campos base: inclui campaign_name, ad_name, adset_name (CORREÇÃO DE BUG)
// quality_ranking etc: só em nível ad
const INSIGHT_FIELDS_BASE = [
  'spend', 'impressions', 'clicks', 'ctr', 'cpc', 'cpm', 'cpp', 'reach', 'frequency',
  'inline_link_clicks', 'outbound_clicks', 'unique_clicks', 'unique_ctr',
  'unique_inline_link_clicks',
  'actions', 'action_values', 'unique_actions',
  'inline_post_engagement', 'social_spend',
  'video_play_actions', 'video_thruplay_watched_actions',
  'video_p25_watched_actions', 'video_p50_watched_actions',
  'video_p75_watched_actions', 'video_p100_watched_actions',
  // Nomes (bug fix da v2.1)
  'campaign_id', 'campaign_name',
  'adset_id', 'adset_name',
  'ad_id', 'ad_name',
  'account_name',
  'objective',
  'quality_ranking', 'engagement_rate_ranking', 'conversion_rate_ranking',
  'date_start', 'date_stop'
].join(',');

const INSIGHT_FIELDS_ACCOUNT = INSIGHT_FIELDS_BASE
  .split(',')
  .filter(function (f) {
    return !['quality_ranking', 'engagement_rate_ranking', 'conversion_rate_ranking'].includes(f);
  })
  .join(',');

const INSIGHT_FIELDS_MINIMAL = [
  'spend', 'impressions', 'clicks', 'reach',
  'actions', 'action_values',
  'inline_link_clicks',
  'campaign_id', 'campaign_name',
  'adset_id', 'adset_name',
  'ad_id', 'ad_name',
  'date_start', 'date_stop'
].join(',');

// ============================================================
// HTTP helpers
// ============================================================
function httpsGet(pathWithQuery) {
  return new Promise(function (resolve, reject) {
    const req = https.request({
      hostname: GRAPH_HOST, path: pathWithQuery, method: 'GET',
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

function graphGet(endpoint, queryParams, retryCount) {
  retryCount = retryCount || 0;
  const qs = new URLSearchParams(queryParams || {});
  qs.set('access_token', META_SYSTEM_USER_TOKEN);
  const fullPath = '/' + GRAPH_API_VERSION + endpoint + '?' + qs.toString();

  return httpsGet(fullPath).then(function (resp) {
    if (resp.status === 200) return safeParse(resp.body);

    const err = safeParse(resp.body);
    const errObj = err.error || {};

    if (errObj.code === 17 || errObj.code === 613 || errObj.code === 4 || resp.status === 429) {
      if (retryCount >= 3) throw new Error('Rate limit persistente');
      console.log('  ⏳ Rate limit (tent ' + (retryCount + 1) + '/3), 60s...');
      return sleep(60000).then(function () { return graphGet(endpoint, queryParams, retryCount + 1); });
    }

    if (errObj.code === 100 && errObj.message && errObj.message.indexOf('breakdown') !== -1) {
      console.log('  ⚠️  Breakdown não suportado');
      return { data: [] };
    }

    if (resp.status >= 500 && resp.status < 600) {
      if (retryCount >= 2) {
        throw new Error('Graph ' + endpoint + ' HTTP ' + resp.status + ' persistente: ' + (errObj.message || ''));
      }
      const waitMs = 5000 * (retryCount + 1);
      console.log('  ⏳ HTTP ' + resp.status + ' (tent ' + (retryCount + 1) + '/2), ' + (waitMs / 1000) + 's...');
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
      console.log('  ⚠️  Limite ' + MAX_PAGES_PER_QUERY + ' páginas em ' + label);
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
// Janelas
// ============================================================
function gerarJanelas(totalDias, chunkDias) {
  const janelas = [];
  const hoje = new Date();
  let offsetFim = 0;

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
  return janelas.reverse();
}

// ============================================================
// coletarInsights
// ============================================================
async function coletarInsights(accountId, level, days, breakdowns, useMinimalFields) {
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
  const chunkLabel = precisaChunkar ? ' [' + janelas.length + ' chunks]' : '';
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
      console.log('    ⚠️  Chunk falhou: ' + err.message);
      if (!useMinimalFields) {
        console.log('    🔄 Tentando com campos mínimos...');
        try {
          const paramsMin = Object.assign({}, params, { fields: INSIGHT_FIELDS_MINIMAL });
          const rows = await graphPaginate('/act_' + accountId + '/insights', paramsMin, subLabel + ' [min]');
          todos.push.apply(todos, rows);
        } catch (err2) {
          console.log('    ❌ Falhou com mínimos: ' + err2.message);
        }
      }
    }
  }

  console.log('    ✅ ' + level + ': ' + todos.length + ' linhas');
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
  console.log('  📋 Metadados campanhas...');
  return graphPaginate('/act_' + accountId + '/campaigns', {
    fields: 'id,name,objective,status,effective_status,buying_type,created_time,start_time,stop_time,daily_budget,lifetime_budget',
    limit: '100'
  }, 'camp act_' + accountId.slice(-4)).catch(function (err) {
    console.log('    ⚠️  ' + err.message); return [];
  });
}

function coletarAdsets(accountId) {
  console.log('  📋 Metadados adsets...');
  return graphPaginate('/act_' + accountId + '/adsets', {
    fields: 'id,name,campaign_id,status,effective_status,optimization_goal,billing_event,bid_strategy,daily_budget,lifetime_budget,created_time',
    limit: '100'
  }, 'adsets act_' + accountId.slice(-4)).catch(function (err) {
    console.log('    ⚠️  ' + err.message); return [];
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
            'creative{id,name,thumbnail_url,video_id,object_type,call_to_action_type}',  // REMOVIDO: body, title, image_url (muito grandes)
    limit: '100'
  };
  if (filtering) params.filtering = filtering;
  return graphPaginate('/act_' + accountId + '/ads', params, 'ads act_' + accountId.slice(-4)).catch(function (err) {
    console.log('    ⚠️  ' + err.message); return [];
  });
}

// ============================================================
// EXTRATOR + SLIMMER
// Retorna apenas o objeto slim (sem os campos brutos)
// ============================================================
function extractActionValue(list, types) {
  if (!list) return 0;
  let sum = 0;
  for (let i = 0; i < list.length; i++) {
    if (types.indexOf(list[i].action_type) !== -1) sum += parseFloat(list[i].value) || 0;
  }
  return sum;
}

const PURCHASE_ALIASES = ['purchase', 'omni_purchase', 'offsite_conversion.fb_pixel_purchase',
                          'onsite_web_purchase', 'onsite_web_app_purchase'];
const ADD_CART_ALIASES = ['add_to_cart', 'omni_add_to_cart', 'offsite_conversion.fb_pixel_add_to_cart'];
const INIT_CHECKOUT_ALIASES = ['initiate_checkout', 'omni_initiated_checkout', 'offsite_conversion.fb_pixel_initiate_checkout'];
const VIEW_CONTENT_ALIASES = ['view_content', 'omni_view_content', 'offsite_conversion.fb_pixel_view_content'];

function slimInsight(row, accountId) {
  const slim = {
    d: row.date_start,
    acc: accountId
  };

  // IDs e nomes — só se existirem
  if (row.campaign_id) slim.cid = row.campaign_id;
  if (row.campaign_name) slim.cnm = row.campaign_name;
  if (row.adset_id) slim.sid = row.adset_id;
  if (row.adset_name) slim.snm = row.adset_name;
  if (row.ad_id) slim.aid = row.ad_id;
  if (row.ad_name) slim.anm = row.ad_name;
  if (row.objective) slim.obj = row.objective;

  // Breakdowns (se presentes)
  if (row.age) slim.age = row.age;
  if (row.gender) slim.gnd = row.gender;
  if (row.publisher_platform) slim.pp = row.publisher_platform;
  if (row.platform_position) slim.pos = row.platform_position;

  // Métricas numéricas (arredondadas pra economizar bytes)
  const spend = parseFloat(row.spend) || 0;
  const impressions = parseInt(row.impressions) || 0;
  const clicks = parseInt(row.clicks) || 0;
  const reach = parseInt(row.reach) || 0;
  const linkClicks = parseInt(row.inline_link_clicks) || 0;

  if (spend > 0) slim.sp = Math.round(spend * 100) / 100;
  if (impressions > 0) slim.im = impressions;
  if (clicks > 0) slim.cl = clicks;
  if (reach > 0) slim.rc = reach;
  if (linkClicks > 0) slim.lc = linkClicks;
  if (row.frequency) slim.fr = Math.round(parseFloat(row.frequency) * 100) / 100;

  // Outbound clicks (lista de ações)
  if (row.outbound_clicks) {
    const oc = row.outbound_clicks.reduce(function (s, x) { return s + (parseFloat(x.value) || 0); }, 0);
    if (oc > 0) slim.oc = oc;
  }

  // Purchases + value
  const purchases = extractActionValue(row.actions, PURCHASE_ALIASES);
  const purchaseValue = extractActionValue(row.action_values, PURCHASE_ALIASES);
  if (purchases > 0) slim.pu = purchases;
  if (purchaseValue > 0) slim.pv = Math.round(purchaseValue * 100) / 100;

  // Funil
  const addToCart = extractActionValue(row.actions, ADD_CART_ALIASES);
  const initCheckout = extractActionValue(row.actions, INIT_CHECKOUT_ALIASES);
  const viewContent = extractActionValue(row.actions, VIEW_CONTENT_ALIASES);
  if (addToCart > 0) slim.atc = addToCart;
  if (initCheckout > 0) slim.ic = initCheckout;
  if (viewContent > 0) slim.vc = viewContent;

  // Vídeo
  const videoPlays = extractActionValue(row.video_play_actions, ['video_view']);
  const videoThruplay = extractActionValue(row.video_thruplay_watched_actions, ['video_view']);
  const videoP25 = extractActionValue(row.video_p25_watched_actions, ['video_view']);
  const videoP50 = extractActionValue(row.video_p50_watched_actions, ['video_view']);
  const videoP75 = extractActionValue(row.video_p75_watched_actions, ['video_view']);
  const videoP100 = extractActionValue(row.video_p100_watched_actions, ['video_view']);

  if (videoPlays > 0) slim.vp = videoPlays;
  if (videoThruplay > 0) slim.vtp = videoThruplay;
  if (videoP25 > 0) slim.vp25 = videoP25;
  if (videoP50 > 0) slim.vp50 = videoP50;
  if (videoP75 > 0) slim.vp75 = videoP75;
  if (videoP100 > 0) slim.vp100 = videoP100;

  // Rankings (só em ad)
  if (row.quality_ranking && row.quality_ranking !== 'UNKNOWN') slim.qr = row.quality_ranking;
  if (row.engagement_rate_ranking && row.engagement_rate_ranking !== 'UNKNOWN') slim.err = row.engagement_rate_ranking;
  if (row.conversion_rate_ranking && row.conversion_rate_ranking !== 'UNKNOWN') slim.crr = row.conversion_rate_ranking;

  return slim;
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
  fs.writeFileSync(CREATIVES_CACHE_FILE, JSON.stringify(cache));  // Compacto também
}

// ============================================================
// Stats (agora usa nomes curtos: sp, pv, pu, etc)
// ============================================================
function computeStats(accountInsights, campaignInsights, adInsights, campaignsMeta) {
  const hoje = new Date();
  const d7 = new Date(hoje.getTime() - 7 * 86400000);
  const d30 = new Date(hoje.getTime() - 30 * 86400000);
  const d90 = new Date(hoje.getTime() - 90 * 86400000);

  // Mapa id→name de campanhas (fallback se insights não trouxer cnm)
  const campNameMap = {};
  campaignsMeta.forEach(function (c) { campNameMap[c.id] = c.name; });

  function aggregate(rows, desde) {
    const filtered = desde ? rows.filter(function (r) {
      return new Date(r.d || 0) >= desde;
    }) : rows;
    let spend = 0, purchases = 0, purchaseValue = 0, impressions = 0, clicks = 0,
        linkClicks = 0, addToCart = 0, initCheckout = 0;
    filtered.forEach(function (r) {
      spend += r.sp || 0;
      purchases += r.pu || 0;
      purchaseValue += r.pv || 0;
      impressions += r.im || 0;
      clicks += r.cl || 0;
      linkClicks += r.lc || 0;
      addToCart += r.atc || 0;
      initCheckout += r.ic || 0;
    });
    return {
      spend: spend, purchases: purchases, purchaseValue: purchaseValue,
      impressions: impressions, clicks: clicks, linkClicks: linkClicks,
      addToCart: addToCart, initCheckout: initCheckout,
      roas: spend > 0 ? purchaseValue / spend : 0,
      cpa: purchases > 0 ? spend / purchases : 0,
      ctr: impressions > 0 ? (clicks / impressions) * 100 : 0
    };
  }

  const byMonth = {};
  accountInsights.forEach(function (r) {
    const m = (r.d || '').slice(0, 7);
    if (!m) return;
    if (!byMonth[m]) byMonth[m] = { spend: 0, purchases: 0, purchaseValue: 0 };
    byMonth[m].spend += r.sp || 0;
    byMonth[m].purchases += r.pu || 0;
    byMonth[m].purchaseValue += r.pv || 0;
  });
  Object.keys(byMonth).forEach(function (m) {
    byMonth[m].roas = byMonth[m].spend > 0 ? byMonth[m].purchaseValue / byMonth[m].spend : 0;
  });

  // Top campanhas
  const byCampaign = {};
  campaignInsights.forEach(function (r) {
    const id = r.cid || 'unknown';
    if (!byCampaign[id]) {
      byCampaign[id] = {
        campaign_id: id,
        campaign_name: r.cnm || campNameMap[id] || '(sem nome)',
        account_id: r.acc,
        spend: 0, purchases: 0, purchaseValue: 0, impressions: 0, clicks: 0
      };
    }
    byCampaign[id].spend += r.sp || 0;
    byCampaign[id].purchases += r.pu || 0;
    byCampaign[id].purchaseValue += r.pv || 0;
    byCampaign[id].impressions += r.im || 0;
    byCampaign[id].clicks += r.cl || 0;
  });
  const campArr = Object.values(byCampaign).map(function (c) {
    c.roas = c.spend > 0 ? c.purchaseValue / c.spend : 0;
    c.cpa = c.purchases > 0 ? c.spend / c.purchases : 0;
    return c;
  });

  const byAd = {};
  adInsights.forEach(function (r) {
    const id = r.aid || 'unknown';
    if (!byAd[id]) {
      byAd[id] = {
        ad_id: id,
        ad_name: r.anm || '(sem nome)',
        campaign_id: r.cid, adset_id: r.sid, account_id: r.acc,
        spend: 0, purchases: 0, purchaseValue: 0, impressions: 0,
        videoPlays: 0, videoThruplay: 0,
        qr: r.qr, err: r.err, crr: r.crr
      };
    }
    byAd[id].spend += r.sp || 0;
    byAd[id].purchases += r.pu || 0;
    byAd[id].purchaseValue += r.pv || 0;
    byAd[id].impressions += r.im || 0;
    byAd[id].videoPlays += r.vp || 0;
    byAd[id].videoThruplay += r.vtp || 0;
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
  console.log('🚀 Suplemind Meta Ads Collector v2.2 (SLIM)');
  console.log('   Modo: ' + MODE);
  console.log('   Timestamp: ' + new Date().toISOString());
  console.log('   Target: <15 MB em meta.json');
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
      version: '2.2-slim',
      collectedAt: new Date().toISOString(),
      mode: MODE,
      graphApiVersion: GRAPH_API_VERSION,
      windows: { main: daysMain, ad: daysAd, breakdown: daysBreakdown },
      // Legenda para ajudar a interpretar os nomes curtos no JSON
      fieldLegend: {
        d: 'date_start',
        acc: 'account_id', cid: 'campaign_id', cnm: 'campaign_name',
        sid: 'adset_id', snm: 'adset_name', aid: 'ad_id', anm: 'ad_name',
        sp: 'spend', im: 'impressions', cl: 'clicks', rc: 'reach', fr: 'frequency',
        lc: 'inline_link_clicks', oc: 'outbound_clicks',
        pu: 'purchases', pv: 'purchase_value',
        atc: 'add_to_cart', ic: 'initiate_checkout', vc: 'view_content',
        vp: 'video_plays', vtp: 'video_thruplay',
        vp25: 'video_p25', vp50: 'video_p50', vp75: 'video_p75', vp100: 'video_p100',
        qr: 'quality_ranking', err: 'engagement_rate_ranking', crr: 'conversion_rate_ranking',
        age: 'age', gnd: 'gender', pp: 'publisher_platform', pos: 'platform_position',
        obj: 'objective'
      }
    },
    accounts: {},
    insights: { account: [], campaign: [], adset: [], ad: [] },
    breakdowns: { byAgeGender: [], byPublisherPlatform: [], byPlatformPosition: [] },
    campaigns: [], adsets: [], ads: [], creatives: {},
    errors: []
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
      console.log('  ❌ Meta: ' + err.message);
      output.errors.push({ account: accId, step: 'metadata', error: err.message });
      continue;
    }
    await sleep(RATE_LIMIT_MS);

    const levels = [
      { name: 'account', days: daysMain },
      { name: 'campaign', days: daysMain },
      { name: 'adset', days: daysMain },
      { name: 'ad', days: daysAd }
    ];
    for (const lv of levels) {
      try {
        const raw = await coletarInsights(accId, lv.name, lv.days, null, false);
        const slim = raw.map(function (r) { return slimInsight(r, accId); });
        output.insights[lv.name].push.apply(output.insights[lv.name], slim);
      } catch (err) {
        console.log('  ❌ insights[' + lv.name + ']: ' + err.message);
        output.errors.push({ account: accId, step: 'insights_' + lv.name, error: err.message });
      }
      await sleep(RATE_LIMIT_MS);
    }

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
          const slim = raw.map(function (r) { return slimInsight(r, accId); });
          output.breakdowns[bc.name].push.apply(output.breakdowns[bc.name], slim);
          console.log('    ✅ ' + bc.name + ': ' + slim.length + ' linhas');
        } catch (err) {
          console.log('    ⚠️  ' + bc.name + ': ' + err.message);
          output.errors.push({ account: accId, step: 'bk_' + bc.name, error: err.message });
        }
        await sleep(RATE_LIMIT_MS);
      }
    }

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

    if (MODE === 'full') {
      try {
        const ads = await coletarAdsECriativos(accId, true);
        ads.forEach(function (a) {
          a.account_id = accId;
          // Slim dos ads: remover o objeto creative aninhado
          const slimAd = {
            id: a.id, name: a.name, status: a.effective_status,
            adset_id: a.adset_id, campaign_id: a.campaign_id,
            account_id: accId,
            creative_id: a.creative ? a.creative.id : null,
            has_video: !!(a.creative && a.creative.video_id)
          };
          output.ads.push(slimAd);

          if (a.creative) {
            output.creatives[a.id] = {
              ad_id: a.id,
              creative_id: a.creative.id,
              name: a.creative.name || a.name,
              thumbnail_url: a.creative.thumbnail_url,
              video_id: a.creative.video_id || null,
              object_type: a.creative.object_type,
              cta: a.creative.call_to_action_type,
              collectedAt: new Date().toISOString()
            };
            creativesCache[a.id] = output.creatives[a.id];
          }
        });
        console.log('  ✅ Ads ativos: ' + ads.length);
      } catch (err) {
        output.errors.push({ account: accId, step: 'ads_criativos', error: err.message });
      }
      await sleep(RATE_LIMIT_MS);
    } else {
      Object.assign(output.creatives, creativesCache);
      console.log('  📥 Criativos cache: ' + Object.keys(creativesCache).length);
    }
  }

  if (Object.keys(output.creatives).length > 0) salvarCacheCriativos(output.creatives);

  console.log('');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  console.log('📊 Stats...');
  console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');

  output.stats = computeStats(output.insights.account, output.insights.campaign, output.insights.ad, output.campaigns);
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
  // SALVAR COMPACTO (sem pretty-print!)
  fs.writeFileSync(DATA_FILE, JSON.stringify(output));
  const sizeMB = (fs.statSync(DATA_FILE).size / 1024 / 1024).toFixed(2);

  console.log('');
  console.log('✅ Coleta Meta v2.2 concluída!');
  console.log('   Arquivo: ' + sizeMB + ' MB (target <15 MB)');

  if (output.errors.length > 0) {
    console.log('');
    console.log('⚠️  ERROS (' + output.errors.length + '):');
    output.errors.forEach(function (e) {
      console.log('   [' + e.step + '] act_' + e.account.slice(-4) + ': ' + e.error.slice(0, 100));
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
  console.log('📊 Por mês:');
  const meses = Object.keys(output.stats.byMonth).sort();
  meses.forEach(function (m) {
    const d = output.stats.byMonth[m];
    console.log('   ' + m + ': R$ ' + d.spend.toFixed(2) +
                ' | Rev R$ ' + d.purchaseValue.toFixed(2) +
                ' | ROAS ' + d.roas.toFixed(2) + 'x');
  });

  console.log('');
  console.log('🎯 Top 5 campanhas por spend:');
  output.stats.topCampaignsBySpend.slice(0, 5).forEach(function (c, i) {
    console.log('   ' + (i + 1) + '. ' + c.campaign_name.slice(0, 60) +
                ' — R$ ' + c.spend.toFixed(2) + ' (ROAS ' + c.roas.toFixed(2) + 'x)');
  });

  console.log('');
  console.log('🎨 Criativos: ' + Object.keys(output.creatives).length +
              ' | Ads com vídeo: ' + output.ads.filter(function (a) { return a.has_video; }).length);
}

main().catch(function (err) {
  console.error('❌ ERRO FATAL: ' + err.message);
  console.error(err.stack);
  process.exit(1);
});
