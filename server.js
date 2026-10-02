// belavita-mp-sync — Sincroniza movimientos de la cuenta de Mercado Pago
// de Belavita hacia Supabase.
//
// Guarda TODO lo que trae la API en ops.reserva_mp_raw_log (tabla de
// diagnóstico, para poder revisar cualquier cosa rara más adelante).
//
// NO toca ops.reserva_mp_movimientos — el ledger de la "Reserva Belavita"
// es de carga manual. Se puede reactivar con APLICAR_RESERVA_AUTO=true,
// pero por defecto está apagado (ver clasificarYAplicarReserva()).
//
// Variables de entorno necesarias (configurar en Railway):
//   MP_ACCESS_TOKEN       → Access Token de producción de la cuenta de MP de Belavita
//   MP_WEBHOOK_SECRET     → Clave secreta que da el panel de MP al configurar
//                           el webhook (Tus integraciones → Webhooks) — NO
//                           es el Access Token, es otra clave distinta
//   SUPABASE_URL          → misma URL que usa belavita-ops
//   SUPABASE_SERVICE_KEY  → Service Role Key de Supabase (NO la anon key —
//                           esta sí puede escribir sin pasar por RLS,
//                           hace falta porque este es un servicio de
//                           backend, no la app del navegador)
//   RUN_SCHEDULER         → "true" para que sincronice solo cada 30 segundos
//                           (también revisa los cobros del posnet en curso)
//
// POSNET INTEGRADO (02/10/2026): en la caja, "Mercado Pago → Posnet" manda
// el monto de la venta al posnet de esa sucursal. Cuando el posnet aprueba,
// Mercado Pago avisa acá y la venta queda pagada: la caja la ve e imprime la
// factura, igual que con la transferencia. Ver la sección POSNET al final.
// El posnet usa su propia aplicación de Mercado Pago (una de "Pagos
// presenciales · Point"): la de siempre no tiene permiso para manejar
// posnets y Mercado Pago responde 403 "At least one policy returned
// UNAUTHORIZED". Variables (las dos opcionales; sin ellas usa las de arriba):
//   MP_POINT_ACCESS_TOKEN   → Access Token de producción de la aplicación del posnet
//   MP_POINT_WEBHOOK_SECRET → la clave secreta de los Webhooks de ESA aplicación
// En esa aplicación, Webhooks: la misma dirección de /webhook-mp, con el
// tópico "Order (Mercado Pago)".
//                           (además de poder pedirlo a mano por POST /sync) —
//                           es el respaldo del webhook para confirmar ventas
//                           pagadas por transferencia simple

const express = require('express');
const fetch = require('node-fetch');
const crypto = require('crypto');
const { createClient } = require('@supabase/supabase-js');

const app = express();
app.use(express.json());

const MP_ACCESS_TOKEN = process.env.MP_ACCESS_TOKEN;
const MP_WEBHOOK_SECRET = process.env.MP_WEBHOOK_SECRET;
// La aplicación del posnet (Point) tiene su propia credencial y su propia
// clave de avisos. Si no están cargadas, se usan las de arriba.
const MP_POINT_ACCESS_TOKEN = process.env.MP_POINT_ACCESS_TOKEN || process.env.MP_ACCESS_TOKEN;
const MP_POINT_WEBHOOK_SECRET = process.env.MP_POINT_WEBHOOK_SECRET || null;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;

if (!MP_ACCESS_TOKEN || !SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error('⚠ Faltan variables de entorno: MP_ACCESS_TOKEN, SUPABASE_URL, SUPABASE_SERVICE_KEY');
}

const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

// Trae pagos/movimientos de la cuenta de MP entre dos fechas (paginado de
// a 50, que es el máximo recomendado por la API de Search de Payments)
async function fetchPagosMP(desde, hasta) {
  const pagos = [];
  let offset = 0;
  const limit = 50;
  while (true) {
    const url = `https://api.mercadopago.com/v1/payments/search?sort=date_created&criteria=desc&range=date_created&begin_date=${encodeURIComponent(desde)}&end_date=${encodeURIComponent(hasta)}&offset=${offset}&limit=${limit}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${MP_ACCESS_TOKEN}` } });
    if (!res.ok) {
      const texto = await res.text();
      throw new Error(`MP API respondió ${res.status}: ${texto}`);
    }
    const data = await res.json();
    const results = data.results || [];
    pagos.push(...results);
    const total = data.paging?.total || 0;
    offset += limit;
    if (offset >= total || results.length === 0) break;
  }
  return pagos;
}

// Guarda cada pago crudo en la tabla de diagnóstico — ON CONFLICT por
// mp_payment_id para no duplicar si se corre el sync dos veces sobre el
// mismo rango de fechas
async function guardarLogCrudo(pagos) {
  if (!pagos.length) return { nuevos: 0 };
  const filas = pagos.map(p => ({
    mp_payment_id: String(p.id),
    monto: p.transaction_amount,
    fecha: p.date_created,
    operation_type: p.operation_type || null,
    status: p.status || null,
    descripcion: p.description || null,
    raw: p,
  }));
  const { error } = await sb.schema('ops').from('reserva_mp_raw_log')
    .upsert(filas, { onConflict: 'mp_payment_id', ignoreDuplicates: true });
  if (error) throw error;
  return { nuevos: filas.length };
}

// Punto de partida: el pago más reciente que ya tenemos guardado, o hace
// 30 días si es la primera vez que corre. MP exige el formato exacto
// yyyy-MM-dd'T'HH:mm:ss.SSSZ (con milisegundos) — la fecha que devuelve
// Supabase no siempre viene en ese formato exacto, así que la
// reconstruimos con un Date real para asegurar que sea válida
async function calcularFechaDesde() {
  const { data } = await sb.schema('ops').from('reserva_mp_raw_log')
    .select('fecha').order('fecha', { ascending: false }).limit(1).maybeSingle();
  if (data?.fecha) return new Date(data.fecha).toISOString();
  const hace30 = new Date();
  hace30.setDate(hace30.getDate() - 30);
  return hace30.toISOString();
}

// Convierte los movimientos de la Reserva (operation_type='partition_transfer')
// que todavía no procesamos en filas reales del ledger de
// ops.reserva_mp_movimientos — es la ÚNICA categoría que confirmamos que
// corresponde 1 a 1 con los movimientos de "Dinero reservado / Retirar"
// que se ven en la pantalla de Reservas de Mercado Pago (se probó contra
// un caso real: $5.000 el 14/7 a las 09:45, coincide exacto).
//
// Todo lo demás queda afuera a propósito:
//  - "Rendimientos" (el ~$340/día) NO llega por esta API — es una
//    función de la billetera personal sin acceso programático público.
//    Tomás prefiere seguir cargándolo a mano con el botón "+ SUMAR" del
//    widget, así que este servicio no lo toca.
//  - Ventas, pagos a proveedores, etc. no son plata de la Reserva, son la
//    operatoria normal de la cuenta — no corresponde sumarlos acá.
//
// ── APAGADO EL 12/8/2026 ────────────────────────────────────────────────
// Este servicio escribía solo en la Reserva Belavita, y eso se decidió
// cortar: la reserva vuelve a ser 100% manual.
//
// El motivo no es que la clasificación estuviera mal. Es que la plata que
// aparecía sola no coincidía con lo que Alejandro veía en Mercado Pago, y
// un saldo que nadie puede explicar es peor que uno que hay que cargar a
// mano. Además el widget de la app dice, textual, "este saldo no se
// actualiza solo" — el sistema estaba contradiciendo su propio cartel.
//
// Lo que NO cambia: el registro crudo en ops.reserva_mp_raw_log se sigue
// guardando igual. Así que sigue estando el detalle completo de lo que
// informa Mercado Pago para poder revisarlo o conciliarlo cuando haga
// falta; lo único que se corta es la escritura automática en el ledger.
//
// Si algún día se quiere volver a activar, se pone APLICAR_RESERVA_AUTO
// en "true" en Railway. Por defecto está apagado: para que se prenda
// tiene que ser una decisión explícita de alguien, no un descuido.
//
const APLICAR_RESERVA_AUTO = process.env.APLICAR_RESERVA_AUTO === 'true';

async function clasificarYAplicarReserva() {
  const { data: pendientes, error } = await sb.schema('ops').from('reserva_mp_raw_log')
    .select('*').eq('operation_type', 'partition_transfer').eq('revisado', false);
  if (error) throw error;
  if (!pendientes || !pendientes.length) return { aplicados: 0, pendientes: 0 };

  if (!APLICAR_RESERVA_AUTO) {
    // Se marcan como revisados igual: si no, cada sincronización volvería
    // a levantar los mismos y el contador crecería para siempre. La fila
    // cruda queda intacta en reserva_mp_raw_log con todo su detalle.
    const ids = pendientes.map(p => p.id);
    const { error: errorUpdate } = await sb.schema('ops').from('reserva_mp_raw_log')
      .update({ revisado: true }).in('id', ids);
    if (errorUpdate) throw errorUpdate;
    return { aplicados: 0, omitidos: pendientes.length };
  }

  const filasLedger = pendientes.map(p => ({
    monto: p.monto,
    motivo: 'Automático · Mercado Pago (reserva)',
    fecha: p.fecha ? new Date(p.fecha).toISOString().split('T')[0] : null,
    origen_mp_payment_id: p.mp_payment_id,
  }));
  const { error: errorInsert } = await sb.schema('ops').from('reserva_mp_movimientos')
    .upsert(filasLedger, { onConflict: 'origen_mp_payment_id', ignoreDuplicates: true });
  if (errorInsert) throw errorInsert;

  const ids = pendientes.map(p => p.id);
  const { error: errorUpdate } = await sb.schema('ops').from('reserva_mp_raw_log')
    .update({ revisado: true }).in('id', ids);
  if (errorUpdate) throw errorUpdate;

  return { aplicados: filasLedger.length };
}

async function sincronizar() {
  const desde = await calcularFechaDesde();
  const hasta = new Date().toISOString();
  const pagos = await fetchPagosMP(desde, hasta);
  const resultado = await guardarLogCrudo(pagos);
  const clasificacion = await clasificarYAplicarReserva();
  const confirmacionVentas = await confirmarVentasPendientesPorPolling();
  const posnet = await revisarCobrosPosnet();
  const detalleReserva = APLICAR_RESERVA_AUTO
    ? `${clasificacion.aplicados} aplicados a la Reserva`
    : `${clasificacion.omitidos || 0} de Reserva NO aplicados (carga manual)`;
  console.log(`[sync] ${new Date().toISOString()} · ${resultado.nuevos} guardados, ${detalleReserva}, ${confirmacionVentas.confirmadas} ventas confirmadas (rango ${desde} → ${hasta})`);
  return { ...resultado, ...clasificacion, ...confirmacionVentas, ...posnet };
}

app.get('/health', (req, res) => res.json({ ok: true }));

// Disparar la sincronización a mano (ej. desde un botón en la app, o para
// probar apenas se configuran las variables de entorno)
app.post('/sync', async (req, res) => {
  try {
    const resultado = await sincronizar();
    res.json({ ok: true, ...resultado });
  } catch (e) {
    console.error(e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`belavita-mp-sync escuchando en :${PORT}`));

// ═══════════════════════════════════════════════════════════════
// WEBHOOK DE MERCADO PAGO — recibe el aviso al instante cuando llega una
// transferencia (a diferencia del /sync de arriba, que solo mira cada 6
// horas). Sirve para que la comandera imprima sola apenas se confirma el
// pago de una venta hecha con "Mercado Pago" (MP Belavita).
//
// Cómo matchea: busca en ventas_pos una venta con medio_pago='mercado_pago',
// estado_pago='pendiente' y el mismo monto exacto, en las últimas 3 horas.
//  - Si encuentra UNA sola → la marca 'confirmado' (index.html la detecta
//    sola por polling y dispara la impresión).
//  - Si no encuentra ninguna → no hace nada (puede ser una transferencia
//    que no es de una venta, ej. un cliente que pagó algo aparte).
//  - Si encuentra DOS O MÁS (dos sucursales pidiendo el mismo monto a la
//    vez) → NO ADIVINA. Las marca 'ambiguo' para que se resuelva a mano
//    desde la app — es la limitación real que ya habíamos hablado.
// ═══════════════════════════════════════════════════════════════

// Verifica que la notificación realmente venga de Mercado Pago, usando la
// clave secreta que te da el panel de MP al configurar el webhook (NO es
// el Access Token). Sin esto, cualquiera podría mandarle una notificación
// falsa a este endpoint diciendo "ya te pagaron".
function validarFirmaMP(req) {
  if (!MP_WEBHOOK_SECRET) {
    console.error('[webhook-mp] Falta MP_WEBHOOK_SECRET — se rechaza la notificación por seguridad');
    return false;
  }
  const xSignature = req.headers['x-signature'];
  const xRequestId = req.headers['x-request-id'];
  const dataId = req.query['data.id'] || req.body?.data?.id;
  if (!xSignature || !xRequestId || !dataId) return false;

  const partes = {};
  xSignature.split(',').forEach(p => {
    const [k, v] = p.split('=');
    if (k && v) partes[k.trim()] = v.trim();
  });
  if (!partes.ts || !partes.v1) return false;

  const manifest = `id:${dataId};request-id:${xRequestId};ts:${partes.ts};`;
  // Vale con la clave de cualquiera de las dos aplicaciones: la de siempre
  // (transferencias) o la del posnet, que firma sus avisos con la suya.
  const bufB = Buffer.from(partes.v1);
  return [MP_WEBHOOK_SECRET, MP_POINT_WEBHOOK_SECRET].filter(Boolean).some(clave => {
    const hmac = crypto.createHmac('sha256', clave).update(manifest).digest('hex');
    // Comparación en tiempo constante — evita filtrar información por
    // cuánto tarda la comparación (buena práctica para comparar firmas)
    const bufA = Buffer.from(hmac);
    return bufA.length === bufB.length && crypto.timingSafeEqual(bufA, bufB);
  });
}

async function intentarConfirmarVenta(pago, prefijoLog = '[webhook-mp]') {
  const monto = pago.transaction_amount;
  const paymentId = String(pago.id || pago.mp_payment_id || '');
  const desde = new Date(Date.now() - 3 * 60 * 60 * 1000).toISOString(); // ventana de 3hs

  // CLAVE: si esta transferencia puntual YA se usó antes para confirmar
  // otra venta, no se vuelve a usar — sin este chequeo, una transferencia
  // vieja podía "confirmar" una venta nueva del mismo monto que en
  // realidad nunca se pagó (bug real detectado en producción: una prueba
  // de $10 confirmó, 10 minutos después, otra venta de $10 sin que hubiera
  // ninguna transferencia nueva).
  if (paymentId) {
    const { data: yaUsado } = await sb.schema('ops').from('ventas_pos')
      .select('id').eq('confirmado_por_mp_payment_id', paymentId).limit(1);
    if (yaUsado && yaUsado.length) return false;
  }

  // Matchea dos casos: una venta normal por Mercado Pago (monto_total) o
  // una venta con pago dividido donde la porción de MP Belavita
  // (monto_mp_belavita) coincide con lo que llegó
  const { data: candidatas, error } = await sb.schema('ops').from('ventas_pos')
    .select('id, sucursal_id, monto_total, monto_mp_belavita, medio_pago, created_at')
    .eq('estado_pago', 'pendiente')
    .gte('created_at', desde)
    .or(`and(medio_pago.eq.mercado_pago,monto_total.eq.${monto}),and(medio_pago.eq.dividido,monto_mp_belavita.eq.${monto})`)
    // Las ventas por POSNET no se confirman por monto: tienen su propio
    // aviso, atado a la venta. Sin este filtro, una transferencia del mismo
    // monto en otro local podía confirmarlas (o volverlas "ambiguo").
    .or('datos_extra->>mp_modo.is.null,datos_extra->>mp_modo.neq.posnet');
  if (error) { console.error(prefijoLog, error); return false; }

  if (!candidatas || candidatas.length === 0) {
    return false;
  }
  if (candidatas.length > 1) {
    await sb.schema('ops').from('ventas_pos').update({ estado_pago: 'ambiguo' })
      .in('id', candidatas.map(c => c.id));
    console.log(`${prefijoLog} Ambigüedad: ${candidatas.length} ventas pendientes por $${monto} — requiere resolución manual`);
    return false;
  }

  await sb.schema('ops').from('ventas_pos').update({
    estado_pago: 'confirmado', pago_confirmado_en: new Date().toISOString(),
    confirmado_por_mp_payment_id: paymentId || null,
  }).eq('id', candidatas[0].id);
  console.log(`${prefijoLog} Venta ${candidatas[0].id} confirmada por transferencia ${paymentId || '(sin id)'} de $${monto}`);
  return true;
}

// Respaldo del webhook: las transferencias simples (sin QR/checkout) no
// siempre disparan el aviso instantáneo de MP — esto ya lo confirmamos
// con una prueba real. Como plan B, cada vez que corre /sync también
// revisa los money_transfer/account_fund recientes contra las ventas
// pendientes, con el mismo criterio de matcheo por monto que el webhook.
// No es instantáneo, pero corriendo cada 30 segundos (ver RUN_SCHEDULER
// más abajo) se acerca bastante.
async function confirmarVentasPendientesPorPolling() {
  const desde = new Date(Date.now() - 3 * 60 * 60 * 1000).toISOString();
  const { data: pagos, error } = await sb.schema('ops').from('reserva_mp_raw_log')
    .select('mp_payment_id, monto, operation_type, status, fecha')
    .in('operation_type', ['money_transfer', 'account_fund'])
    .eq('status', 'approved')
    .gte('fecha', desde);
  if (error) { console.error('[polling]', error); return { confirmadas: 0 }; }
  if (!pagos || !pagos.length) return { confirmadas: 0 };

  let confirmadas = 0;
  for (const p of pagos) {
    const ok = await intentarConfirmarVenta({ transaction_amount: p.monto, id: p.mp_payment_id }, '[polling]');
    if (ok) confirmadas++;
  }
  return { confirmadas };
}

app.post('/webhook-mp', async (req, res) => {
  // Responder rápido (MP espera 200 en menos de 22 segundos) — el
  // procesamiento real sigue después, sin bloquear la respuesta
  res.sendStatus(200);

  try {
    if (!validarFirmaMP(req)) {
      console.error('[webhook-mp] Firma inválida — notificación ignorada');
      return;
    }
    const dataId = req.query['data.id'] || req.body?.data?.id;
    const type = req.query['type'] || req.body?.type;
    // El posnet avisa como "order": se trae la orden y se aplica a su venta.
    if (type === 'order' && dataId) {
      const orden = await mp('GET', `/v1/orders/${encodeURIComponent(dataId)}`);
      const r = await aplicarOrdenPosnet(orden);
      console.log(`[webhook-mp] posnet ${dataId} → ${r || 'no es de una venta'}`);
      return;
    }
    if (type !== 'payment' || !dataId) return; // no nos interesan otros tópicos

    const resPago = await fetch(`https://api.mercadopago.com/v1/payments/${dataId}`, {
      headers: { Authorization: `Bearer ${MP_ACCESS_TOKEN}` },
    });
    if (!resPago.ok) { console.error('[webhook-mp] no se pudo traer el pago', dataId); return; }
    const pago = await resPago.json();

    if (pago.status !== 'approved') return;
    // Solo transferencias/acreditaciones de dinero — no tarjetas ni otros
    // medios que ya tienen su propio flujo
    if (!['money_transfer', 'account_fund'].includes(pago.operation_type)) return;

    await intentarConfirmarVenta(pago);
  } catch (e) {
    console.error('[webhook-mp] error', e);
  }
});

// Sincronización automática cada 6 horas, solo si se activa explícito
// Antes corría cada 6 horas (alcanzaba para la Reserva) — ahora corre
// cada 30 segundos, porque además sirve de respaldo del webhook para
// confirmar ventas pagadas por transferencia simple (que no siempre
// dispara el aviso instantáneo de MP)
if (process.env.RUN_SCHEDULER === 'true') {
  const TREINTA_SEGUNDOS = 30 * 1000;
  setInterval(() => { sincronizar().catch(e => console.error('[sync automático]', e)); }, TREINTA_SEGUNDOS);
  console.log('Scheduler activado — sincroniza cada 30 segundos');
}

// ═══════════════════════════════════════════════════════════════
// POSNET INTEGRADO — Mercado Pago Point (02/10/2026)
//
// En la caja: "Mercado Pago" → "Posnet" → Finalizar. La venta se guarda
// pendiente, marcada con datos_extra.mp_modo = 'posnet', y Cyron pide acá
// POST /posnet/cobrar. Esto manda el monto al posnet de esa sucursal (la
// tabla ops.posnets dice cuál es) como una "orden" de Mercado Pago, con la
// venta como referencia: venta-<id>. El cliente paga en el posnet como
// quiera (débito, crédito, QR). Mercado Pago avisa a /webhook-mp con el
// tópico "order", y la venta pasa a 'confirmado': la caja la ve (consulta
// cada 3 segundos) e imprime la factura en la Hasar.
//
// A diferencia de la transferencia, NO se adivina por monto: el cobro va
// atado a la venta, así que dos locales cobrando lo mismo a la vez no se
// confunden nunca.
//
// Lo que cuida:
//  · Un cobro por intento, nunca dos: la clave de idempotencia es
//    venta-<id>-intento-<n>. Si se corta internet justo después de crearlo,
//    al reintentar Mercado Pago devuelve la misma orden en vez de otra.
//  · Si ya hay un cobro vivo en el posnet, no se manda otro.
//  · Un aviso viejo (de un intento anterior que venció) no pisa el estado
//    del intento actual. Un pago aprobado, en cambio, siempre cuenta.
//  · Si el aviso no llega, el respaldo de cada 30 segundos consulta la
//    orden directamente.
//  · Solo puede pedir cobros un usuario de Cyron con sesión válida; vincular
//    posnets y cambiarles el modo, solo un administrador.
//
// El posnet tiene que estar en modo PDV (integrado). En modo STANDALONE
// Mercado Pago no le manda órdenes. El modo se cambia desde Cyron
// (Administración → Posnets), y si un día se cae internet, desde ahí mismo
// se lo vuelve al modo normal para seguir cobrando con tarjeta a mano.
// ═══════════════════════════════════════════════════════════════
const MP_API = 'https://api.mercadopago.com';

async function mp(metodo, ruta, cuerpo, idempotencia) {
  const r = await fetch(MP_API + ruta, {
    method: metodo,
    headers: {
      Authorization: `Bearer ${MP_POINT_ACCESS_TOKEN}`, 'Content-Type': 'application/json',
      ...(idempotencia ? { 'X-Idempotency-Key': idempotencia } : {}),
    },
    body: cuerpo ? JSON.stringify(cuerpo) : undefined,
  });
  const texto = await r.text();
  let datos = null;
  try { datos = texto ? JSON.parse(texto) : null; } catch (e) { datos = { crudo: texto }; }
  if (!r.ok) {
    const motivo = (datos && (datos.message || datos.error || (datos.errors && JSON.stringify(datos.errors)))) || texto;
    // Los dos 403 que se entienden mejor en castellano.
    let texto403 = null;
    if (r.status === 403 && /policy returned UNAUTHORIZED/i.test(String(motivo))) {
      texto403 = 'Mercado Pago no le da permiso a esta credencial para manejar posnets. Hace falta el Access Token de una aplicación de "Pagos presenciales · Point" (MP_POINT_ACCESS_TOKEN en Railway).';
    } else if (r.status === 403 && /store_pos_not_found/i.test(JSON.stringify(datos || {}))) {
      texto403 = 'Ese posnet no tiene un local y una caja asignados en Mercado Pago. Asignáselos desde la app de Mercado Pago (Tu negocio → Locales y cajas).';
    }
    const err = new Error(texto403 || `Mercado Pago respondió ${r.status}: ${String(motivo).slice(0, 240)}`);
    err.status = r.status; err.datos = datos;
    throw err;
  }
  return datos;
}

// Cyron llama desde otra dirección (su web y la app de escritorio).
app.use('/posnet', (req, res, next) => {
  res.setHeader('Access-Control-Allow-Origin', req.headers.origin || '*');
  res.setHeader('Vary', 'Origin');
  res.setHeader('Access-Control-Allow-Headers', 'Authorization, Content-Type');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  if (req.method === 'OPTIONS') return res.sendStatus(204);
  next();
});

// Quién pide: la sesión de Cyron (la misma de Supabase) y su usuario activo.
async function usuarioDe(req) {
  const jwt = String(req.headers.authorization || '').replace(/^Bearer\s+/i, '').trim();
  if (!jwt) return null;
  const { data, error } = await sb.auth.getUser(jwt);
  const email = data?.user?.email;
  if (error || !email) return null;
  const { data: u } = await sb.schema('ops').from('usuarios')
    .select('id, nombre, email, activo, rol_acceso, permisos').ilike('email', email).maybeSingle();
  if (!u || u.activo === false) return null;
  return u;
}
const esAdmin = u => !!u && (u.permisos?.admin === true || /^(due|admin)/i.test(String(u.rol_acceso || '')));

async function posnetDeSucursal(sucursalId) {
  const { data, error } = await sb.schema('ops').from('posnets')
    .select('terminal_id, nombre').eq('sucursal_id', sucursalId).eq('proveedor', 'mercadopago').eq('activo', true)
    .limit(1).maybeSingle();
  if (error) throw error;
  return data;
}

// Anota el estado del cobro en datos_extra.posnet (sin pisar lo demás).
async function anotarPosnet(ventaId, cambios, columnas = {}) {
  const { data: v, error: e1 } = await sb.schema('ops').from('ventas_pos').select('datos_extra').eq('id', ventaId).maybeSingle();
  if (e1) throw e1;
  const de = (v && v.datos_extra) || {};
  const posnet = { ...(de.posnet || {}), ...cambios, actualizado: new Date().toISOString() };
  const { error } = await sb.schema('ops').from('ventas_pos').update({ datos_extra: { ...de, posnet }, ...columnas }).eq('id', ventaId);
  if (error) throw error;
  return posnet;
}

const ESTADOS_VIVOS = ['en_posnet', 'procesando'];
const ESTADO_ORDEN = {
  created: 'en_posnet', at_terminal: 'en_posnet', action_required: 'en_posnet', processing: 'procesando',
  canceled: 'cancelado', expired: 'vencido', failed: 'rechazado', refunded: 'devuelto',
};

// Aplica lo que dice Mercado Pago de una orden a su venta. Lo usan el aviso,
// el respaldo de cada 30 segundos y la cancelación.
async function aplicarOrdenPosnet(orden) {
  const m = String(orden?.external_reference || '').match(/^venta-(\d+)$/);
  if (!m) return null;
  const ventaId = Number(m[1]);
  const { data: v, error } = await sb.schema('ops').from('ventas_pos').select('estado_pago, datos_extra').eq('id', ventaId).maybeSingle();
  if (error) throw error;
  if (!v) return null;
  const actual = (v.datos_extra && v.datos_extra.posnet) || {};
  const pago = (orden.transactions && orden.transactions.payments && orden.transactions.payments[0]) || {};
  const st = String(orden.status || '').toLowerCase();

  if (st === 'processed') {
    // Pagado. Cuenta aunque sea de un intento anterior: esa plata entró.
    const pm = pago.payment_method || {};
    const datos = { estado: 'aprobado', order_id: orden.id, tipo: pm.type || null, marca: pm.id || null,
      cuotas: pm.installments || null, pago_id: (pago.reference && pago.reference.id) || pago.id || null, detalle: orden.status_detail || null };
    if (v.estado_pago === 'confirmado') { await anotarPosnet(ventaId, datos); return 'aprobado'; }
    await anotarPosnet(ventaId, datos, {
      estado_pago: 'confirmado', pago_confirmado_en: new Date().toISOString(),
      confirmado_por_mp_payment_id: String(datos.pago_id || orden.id),
    });
    // Si había otro intento vivo en el posnet, se cancela: ya está pagada.
    if (actual.order_id && actual.order_id !== orden.id && ESTADOS_VIVOS.includes(actual.estado)) {
      try { await mp('POST', `/v1/orders/${encodeURIComponent(actual.order_id)}/cancel`, null, `cancelar-${actual.order_id}`); } catch (e) { /* ya no estaba */ }
    }
    return 'aprobado';
  }
  // Un aviso de un intento anterior no pisa el estado del intento actual.
  if (actual.order_id && actual.order_id !== orden.id) return 'viejo';
  if (v.estado_pago === 'confirmado') return 'ya pagada';
  const estado = ESTADO_ORDEN[st] || st || 'desconocido';
  await anotarPosnet(ventaId, { estado, order_id: orden.id, detalle: orden.status_detail || pago.status_detail || null });
  return estado;
}

// Respaldo: si un aviso no llegó, cada 30 segundos se consulta la orden.
async function revisarCobrosPosnet() {
  const desde = new Date(Date.now() - 3 * 60 * 60 * 1000).toISOString();
  const { data, error } = await sb.schema('ops').from('ventas_pos')
    .select('id, datos_extra').eq('estado_pago', 'pendiente').gte('created_at', desde).eq('datos_extra->>mp_modo', 'posnet');
  if (error) { console.error('[posnet/respaldo]', error.message); return { posnet_revisados: 0 }; }
  let revisados = 0;
  for (const v of data || []) {
    const p = (v.datos_extra && v.datos_extra.posnet) || {};
    if (!p.order_id || !ESTADOS_VIVOS.includes(p.estado)) continue;
    try { await aplicarOrdenPosnet(await mp('GET', `/v1/orders/${encodeURIComponent(p.order_id)}`)); revisados++; }
    catch (e) { console.error('[posnet/respaldo]', v.id, e.message); }
  }
  return { posnet_revisados: revisados };
}

async function ventaParaPosnet(id) {
  const { data: v, error } = await sb.schema('ops').from('ventas_pos')
    .select('id, sucursal_id, monto_total, monto_mp_belavita, medio_pago, estado_pago, datos_extra, cancelada').eq('id', id).maybeSingle();
  if (error) throw error;
  return v;
}

// La caja pide cobrar una venta en el posnet de su sucursal.
app.post('/posnet/cobrar', async (req, res) => {
  try {
    const u = await usuarioDe(req);
    if (!u) return res.status(401).json({ ok: false, error: 'La sesión de Cyron no es válida. Volvé a entrar.' });
    const v = await ventaParaPosnet(Number(req.body && req.body.venta_id));
    if (!v) return res.status(404).json({ ok: false, error: 'No encuentro esa venta.' });
    if (v.cancelada) return res.status(409).json({ ok: false, error: 'Esa venta está anulada.' });
    if (v.estado_pago === 'confirmado') return res.json({ ok: true, ya_pagada: true, estado: 'aprobado' });
    if (!v.datos_extra || v.datos_extra.mp_modo !== 'posnet') return res.status(400).json({ ok: false, error: 'Esa venta no es por posnet.' });
    const monto = Number(v.medio_pago === 'dividido' ? v.monto_mp_belavita : v.monto_total);
    if (!(monto > 0)) return res.status(400).json({ ok: false, error: 'La venta no tiene monto.' });
    const t = await posnetDeSucursal(v.sucursal_id);
    if (!t) return res.status(409).json({ ok: false, error: 'Esta sucursal no tiene un posnet vinculado.' });

    const prev = v.datos_extra.posnet || {};
    if (prev.order_id && ESTADOS_VIVOS.includes(prev.estado)) {
      return res.json({ ok: true, order_id: prev.order_id, estado: prev.estado, repetido: true });
    }
    const intento = (Number(prev.intento) || 0) + 1;
    const orden = await mp('POST', '/v1/orders', {
      type: 'point',
      external_reference: `venta-${v.id}`,
      description: `Bela Vita · venta ${v.id}`,
      transactions: { payments: [{ amount: monto.toFixed(2) }] },
      config: { point: { terminal_id: t.terminal_id, print_on_terminal: 'no_ticket' } },
    }, `venta-${v.id}-intento-${intento}`);
    await anotarPosnet(v.id, { order_id: orden.id, terminal_id: t.terminal_id, posnet: t.nombre || null,
      estado: 'en_posnet', intento, enviado_por: u.nombre || u.email, detalle: null });
    console.log(`[posnet] venta ${v.id} → ${t.terminal_id} · $${monto} · orden ${orden.id}`);
    res.json({ ok: true, order_id: orden.id, estado: 'en_posnet' });
  } catch (e) {
    console.error('[posnet/cobrar]', e.message);
    res.status(e.status && e.status < 500 ? 422 : 500).json({ ok: false, error: e.message });
  }
});

// Cancelar el cobro que está en el posnet (si todavía no se pagó).
app.post('/posnet/cancelar', async (req, res) => {
  try {
    const u = await usuarioDe(req);
    if (!u) return res.status(401).json({ ok: false, error: 'La sesión de Cyron no es válida. Volvé a entrar.' });
    const v = await ventaParaPosnet(Number(req.body && req.body.venta_id));
    if (!v) return res.status(404).json({ ok: false, error: 'No encuentro esa venta.' });
    const p = (v.datos_extra && v.datos_extra.posnet) || {};
    if (v.estado_pago === 'confirmado') return res.json({ ok: false, estado: 'aprobado', error: 'Ya estaba pagada: no se cancela.' });
    if (!p.order_id || !ESTADOS_VIVOS.includes(p.estado)) {
      await anotarPosnet(v.id, { estado: 'cancelado', detalle: 'Cancelado desde la caja' });
      return res.json({ ok: true, estado: 'cancelado' });
    }
    try {
      await mp('POST', `/v1/orders/${encodeURIComponent(p.order_id)}/cancel`, null, `cancelar-${p.order_id}`);
      await anotarPosnet(v.id, { estado: 'cancelado', detalle: 'Cancelado desde la caja' });
      return res.json({ ok: true, estado: 'cancelado' });
    } catch (e) {
      // No se pudo cancelar: puede que el cliente ya haya pagado. Se mira la orden.
      const estado = await aplicarOrdenPosnet(await mp('GET', `/v1/orders/${encodeURIComponent(p.order_id)}`));
      return res.json({ ok: estado !== 'aprobado', estado, error: estado === 'aprobado' ? 'El cliente ya pagó: no se cancela.' : null });
    }
  } catch (e) {
    console.error('[posnet/cancelar]', e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Administración: los posnets de la cuenta, a qué local está cada uno y su modo.
app.get('/posnet/terminales', async (req, res) => {
  try {
    const u = await usuarioDe(req);
    if (!esAdmin(u)) return res.status(403).json({ ok: false, error: 'Solo un administrador.' });
    const r = await mp('GET', '/terminals/v1/list?limit=50');
    const terminales = (r && r.data && r.data.terminals) || [];
    const { data: vinculos } = await sb.schema('ops').from('posnets').select('terminal_id, sucursal_id, nombre, activo, proveedor');
    res.json({ ok: true, terminales: terminales.map(t => ({ id: t.id, modo: t.operating_mode,
      vinculo: (vinculos || []).find(x => x.terminal_id === t.id) || null })) });
  } catch (e) {
    console.error('[posnet/terminales]', e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
});

app.post('/posnet/vincular', async (req, res) => {
  try {
    const u = await usuarioDe(req);
    if (!esAdmin(u)) return res.status(403).json({ ok: false, error: 'Solo un administrador.' });
    const { terminal_id, sucursal_id, nombre } = req.body || {};
    if (!terminal_id) return res.status(400).json({ ok: false, error: 'Falta el posnet.' });
    if (!sucursal_id) {
      const { error } = await sb.schema('ops').from('posnets').update({ activo: false }).eq('terminal_id', terminal_id);
      if (error) throw error;
      return res.json({ ok: true, desvinculado: true });
    }
    // Un local, un posnet de Mercado Pago activo: el anterior de ese local se desactiva.
    await sb.schema('ops').from('posnets').update({ activo: false }).eq('sucursal_id', sucursal_id).eq('proveedor', 'mercadopago').neq('terminal_id', terminal_id);
    const { error } = await sb.schema('ops').from('posnets').upsert({ terminal_id, sucursal_id, nombre: nombre || null,
      proveedor: 'mercadopago', activo: true }, { onConflict: 'terminal_id' });
    if (error) throw error;
    res.json({ ok: true });
  } catch (e) {
    console.error('[posnet/vincular]', e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// Modo integrado (PDV) o normal (STANDALONE). El posnet toma el cambio al reiniciarse.
app.post('/posnet/modo', async (req, res) => {
  try {
    const u = await usuarioDe(req);
    if (!esAdmin(u)) return res.status(403).json({ ok: false, error: 'Solo un administrador.' });
    const { terminal_id, modo } = req.body || {};
    if (!terminal_id || !['PDV', 'STANDALONE'].includes(modo)) return res.status(400).json({ ok: false, error: 'Falta el posnet o el modo.' });
    const r = await mp('PATCH', '/terminals/v1/setup', { terminals: [{ id: terminal_id, operating_mode: modo }] });
    res.json({ ok: true, resultado: r });
  } catch (e) {
    console.error('[posnet/modo]', e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
});
