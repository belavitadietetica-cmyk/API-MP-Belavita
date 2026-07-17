// belavita-mp-sync — Sincroniza movimientos de la cuenta de Mercado Pago
// de Belavita hacia Supabase.
//
// Guarda TODO lo que trae la API en ops.reserva_mp_raw_log (tabla de
// diagnóstico, para poder revisar cualquier cosa rara más adelante), y
// además aplica automáticamente a ops.reserva_mp_movimientos (el ledger
// real que usa el widget "Reserva Belavita" en la app) SOLO los
// movimientos de tipo "partition_transfer" — confirmados 1 a 1 contra la
// pantalla de Reservas de Mercado Pago (ver clasificarYAplicarReserva()
// más abajo para el detalle de qué se automatiza y qué no).
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
async function clasificarYAplicarReserva() {
  const { data: pendientes, error } = await sb.schema('ops').from('reserva_mp_raw_log')
    .select('*').eq('operation_type', 'partition_transfer').eq('revisado', false);
  if (error) throw error;
  if (!pendientes || !pendientes.length) return { aplicados: 0 };

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
  console.log(`[sync] ${new Date().toISOString()} · ${resultado.nuevos} guardados, ${clasificacion.aplicados} aplicados a la Reserva, ${confirmacionVentas.confirmadas} ventas confirmadas (rango ${desde} → ${hasta})`);
  return { ...resultado, ...clasificacion, ...confirmacionVentas };
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
  const hmac = crypto.createHmac('sha256', MP_WEBHOOK_SECRET).update(manifest).digest('hex');
  // Comparación en tiempo constante — evita filtrar información por
  // cuánto tarda la comparación (buena práctica para comparar firmas)
  const bufA = Buffer.from(hmac);
  const bufB = Buffer.from(partes.v1);
  return bufA.length === bufB.length && crypto.timingSafeEqual(bufA, bufB);
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

  const { data: candidatas, error } = await sb.schema('ops').from('ventas_pos')
    .select('id, sucursal_id, monto_total, created_at')
    .eq('medio_pago', 'mercado_pago')
    .eq('estado_pago', 'pendiente')
    .eq('monto_total', monto)
    .gte('created_at', desde);
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
