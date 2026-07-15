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
//   SUPABASE_URL          → misma URL que usa belavita-ops
//   SUPABASE_SERVICE_KEY  → Service Role Key de Supabase (NO la anon key —
//                           esta sí puede escribir sin pasar por RLS,
//                           hace falta porque este es un servicio de
//                           backend, no la app del navegador)
//   RUN_SCHEDULER         → "true" para que sincronice solo cada 6 horas
//                           además de poder pedirlo a mano por POST /sync

const express = require('express');
const fetch = require('node-fetch');
const { createClient } = require('@supabase/supabase-js');

const app = express();
app.use(express.json());

const MP_ACCESS_TOKEN = process.env.MP_ACCESS_TOKEN;
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
// 30 días si es la primera vez que corre
async function calcularFechaDesde() {
  const { data } = await sb.schema('ops').from('reserva_mp_raw_log')
    .select('fecha').order('fecha', { ascending: false }).limit(1).maybeSingle();
  if (data?.fecha) return data.fecha;
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
  console.log(`[sync] ${new Date().toISOString()} · ${resultado.nuevos} guardados, ${clasificacion.aplicados} aplicados a la Reserva (rango ${desde} → ${hasta})`);
  return { ...resultado, ...clasificacion };
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

// Sincronización automática cada 6 horas, solo si se activa explícito
if (process.env.RUN_SCHEDULER === 'true') {
  const SEIS_HORAS = 6 * 60 * 60 * 1000;
  setInterval(() => { sincronizar().catch(e => console.error('[sync automático]', e)); }, SEIS_HORAS);
  console.log('Scheduler activado — sincroniza cada 6 horas');
}
