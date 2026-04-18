// inspect_nfe.js — Script de inspeção do schema da NFe do Bling v3
// USO: node inspect_nfe.js
// Lê credenciais dos secrets, pega 1 NFe da lista, consulta detalhe completo,
// imprime estrutura anonimizada para descobrir os nomes reais dos campos.

const https = require('https');

const BLING_CLIENT_ID = process.env.BLING_CLIENT_ID;
const BLING_CLIENT_SECRET = process.env.BLING_CLIENT_SECRET;
const BLING_REFRESH_TOKEN = process.env.BLING_REFRESH_TOKEN;

function httpRequest(options, body) {
  return new Promise(function (resolve, reject) {
    const req = https.request(options, function (res) {
      let chunks = '';
      res.on('data', function (c) { chunks += c; });
      res.on('end', function () { resolve({ status: res.statusCode, body: chunks }); });
    });
    req.on('error', reject);
    if (body) req.write(body);
    req.end();
  });
}

function sleep(ms) { return new Promise(function (r) { setTimeout(r, ms); }); }

function refreshToken() {
  const auth = Buffer.from(BLING_CLIENT_ID + ':' + BLING_CLIENT_SECRET).toString('base64');
  const body = 'grant_type=refresh_token&refresh_token=' + encodeURIComponent(BLING_REFRESH_TOKEN);
  return httpRequest({
    hostname: 'www.bling.com.br',
    path: '/Api/v3/oauth/token',
    method: 'POST',
    headers: {
      'Authorization': 'Basic ' + auth,
      'Content-Type': 'application/x-www-form-urlencoded',
      'Content-Length': Buffer.byteLength(body),
      'Accept': '1.0'
    }
  }, body).then(function (r) {
    if (r.status !== 200) throw new Error('OAuth ' + r.status + ': ' + r.body);
    return JSON.parse(r.body).access_token;
  });
}

function apiGet(token, endpoint) {
  return httpRequest({
    hostname: 'www.bling.com.br',
    path: '/Api/v3' + endpoint,
    method: 'GET',
    headers: {
      'Authorization': 'Bearer ' + token,
      'Accept': 'application/json'
    }
  }, null).then(function (r) {
    if (r.status === 429) return sleep(2000).then(function () { return apiGet(token, endpoint); });
    if (r.status !== 200) throw new Error(endpoint + ' ' + r.status + ': ' + r.body.slice(0, 300));
    return JSON.parse(r.body);
  });
}

function sampleStructure(obj, depth, maxDepth) {
  depth = depth || 0;
  maxDepth = maxDepth || 4;
  if (obj === null) return 'null';
  if (obj === undefined) return 'undefined';
  if (Array.isArray(obj)) {
    if (obj.length === 0) return '[]';
    return '[ ' + sampleStructure(obj[0], depth + 1, maxDepth) + ' ... (' + obj.length + ' items) ]';
  }
  if (typeof obj === 'object') {
    if (depth >= maxDepth) return '{...}';
    const result = {};
    for (const key in obj) {
      result[key] = sampleStructure(obj[key], depth + 1, maxDepth);
    }
    return result;
  }
  if (typeof obj === 'string') {
    if (obj.length > 30) return '"' + obj.slice(0, 30) + '..."';
    return '"' + obj + '"';
  }
  return String(obj) + ' (' + typeof obj + ')';
}

function main() {
  console.log('🔍 Inspeção de schema — Bling API v3');
  console.log('');

  let token;
  return refreshToken().then(function (t) {
    token = t;
    console.log('✅ Token OK');

    console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    console.log('1️⃣  SCHEMA DA LISTAGEM: GET /nfe?limite=3&pagina=1');
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    return apiGet(token, '/nfe?limite=3&pagina=1&tipo=1');
  }).then(function (listResp) {
    const nfes = (listResp && listResp.data) || [];
    console.log('Total retornado: ' + nfes.length + ' NFes');
    console.log('');
    if (nfes.length > 0) {
      console.log('📋 Estrutura da primeira NFe (listagem):');
      console.log(JSON.stringify(sampleStructure(nfes[0]), null, 2));
      console.log('');
      console.log('🔑 Todos os campos top-level da listagem:');
      console.log('   ' + Object.keys(nfes[0]).join(', '));
      console.log('');
      const nfeId = nfes[0].id;
      console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
      console.log('2️⃣  SCHEMA DO DETALHE: GET /nfe/' + nfeId);
      console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
      return apiGet(token, '/nfe/' + nfeId);
    }
    throw new Error('Lista de NFes vazia');
  }).then(function (detailResp) {
    const nfe = detailResp && detailResp.data;
    if (nfe) {
      console.log('📋 Estrutura do DETALHE de 1 NFe:');
      console.log(JSON.stringify(sampleStructure(nfe), null, 2));
      console.log('');
      console.log('🔑 Todos os campos top-level do detalhe:');
      console.log('   ' + Object.keys(nfe).join(', '));
      console.log('');

      console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
      console.log('3️⃣  ANÁLISE DE CAMPOS CRÍTICOS');
      console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');

      const checkField = function (path) {
        const parts = path.split('.');
        let cur = nfe;
        for (let i = 0; i < parts.length; i++) {
          if (cur === null || cur === undefined) return '❌ null/undefined';
          cur = cur[parts[i]];
        }
        if (cur === undefined) return '❌ campo não existe';
        if (cur === null) return '⚠️  null';
        if (typeof cur === 'object') return '✅ existe (' + (Array.isArray(cur) ? 'array' : 'object') + ')';
        return '✅ = ' + JSON.stringify(cur).slice(0, 50);
      };

      console.log('Valor da NFe:');
      console.log('  valorNota:          ' + checkField('valorNota'));
      console.log('  valor:              ' + checkField('valor'));
      console.log('  total:              ' + checkField('total'));
      console.log('  totalProdutos:      ' + checkField('totalProdutos'));

      console.log('\nSituação:');
      console.log('  situacao:           ' + checkField('situacao'));
      console.log('  situacao.id:        ' + checkField('situacao.id'));

      console.log('\nTipo (saída/entrada):');
      console.log('  tipo:               ' + checkField('tipo'));

      console.log('\nData emissão:');
      console.log('  dataEmissao:        ' + checkField('dataEmissao'));
      console.log('  data:               ' + checkField('data'));

      console.log('\nContato (cliente):');
      console.log('  contato:            ' + checkField('contato'));
      console.log('  contato.tipoPessoa: ' + checkField('contato.tipoPessoa'));
      console.log('  contato.numeroDocumento: ' + checkField('contato.numeroDocumento'));
      console.log('  contato.nome:       ' + checkField('contato.nome'));

      console.log('\nVinculação com pedido:');
      console.log('  idOrigem:           ' + checkField('idOrigem'));
      console.log('  numeroPedidoLoja:   ' + checkField('numeroPedidoLoja'));
      console.log('  numeroLoja:         ' + checkField('numeroLoja'));
      console.log('  pedido:             ' + checkField('pedido'));
      console.log('  pedidoVenda:        ' + checkField('pedidoVenda'));
      console.log('  idPedido:           ' + checkField('idPedido'));

      console.log('\nLoja:');
      console.log('  loja:               ' + checkField('loja'));
      console.log('  loja.id:            ' + checkField('loja.id'));

      console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
      console.log('4️⃣  TESTE EM MAIS 5 NFes — buscar campos de vínculo');
      console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    } else {
      console.log('❌ Detalhe da NFe veio vazio');
    }
  }).then(function () {
    return apiGet(token, '/nfe?limite=20&pagina=1&tipo=1');
  }).then(function (listResp) {
    const nfes = (listResp && listResp.data) || [];

    let achouVinculo = null;
    for (let i = 0; i < nfes.length && i < 5; i++) {
      const keys = Object.keys(nfes[i]);
      const vinculoKeys = keys.filter(function (k) { return k.toLowerCase().indexOf('pedido') !== -1 || k.toLowerCase().indexOf('origem') !== -1; });
      if (vinculoKeys.length > 0) {
        achouVinculo = { nfe: nfes[i], vinculoKeys: vinculoKeys };
        break;
      }
    }

    if (achouVinculo) {
      console.log('🔗 Campos relacionados a vínculo/pedido encontrados na listagem:');
      console.log('   ' + achouVinculo.vinculoKeys.join(', '));
      achouVinculo.vinculoKeys.forEach(function (k) {
        console.log('   ' + k + ' = ' + JSON.stringify(achouVinculo.nfe[k]).slice(0, 100));
      });
    } else {
      console.log('⚠️  Nenhum campo "pedido*" ou "origem*" encontrado na listagem das 5 primeiras NFes.');
      console.log('    Os campos da listagem são:');
      if (nfes.length > 0) console.log('    ' + Object.keys(nfes[0]).join(', '));
    }

    console.log('\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    console.log('5️⃣  FIM DA INSPEÇÃO');
    console.log('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    console.log('Envie esse log ao Claude para ajustar o collect.js v5.3');
  }).catch(function (err) {
    console.error('❌ ERRO: ' + err.message);
    console.error(err.stack);
    process.exit(1);
  });
}

main();
