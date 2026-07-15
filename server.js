// belavita-mp-sync — Sincroniza movimientos de la cuenta de Mercado Pago
// de Belavita hacia Supabase. Arranca en MODO DIAGNÓSTICO: todo lo que
// trae de la API de MP lo guarda en ops.reserva_mp_raw_log (una tabla
// aparte, de solo lectura para nosotros), SIN tocar todavía
// ops.reserva_mp_movimientos (el ledger real que usa el widget "Reserva
// Belavita" en la app). La idea es primero VER qué tipo de movimientos
// trae la cuenta real (ventas, rendimiento diario, retiros, etc.) y
// juntos decidir cuáles se suman automáticamente a la Reserva antes de
// activar eso — así no se mete un número mal clasificado en un ledger de
// plata real sin haberlo revisado antes.
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

async function sincronizar() {
  const desde = await calcularFechaDesde();
  const hasta = new Date().toISOString();
  const pagos = await fetchPagosMP(desde, hasta);
  const resultado = await guardarLogCrudo(pagos);
  console.log(`[sync] ${new Date().toISOString()} · ${resultado.nuevos} movimientos guardados (rango ${desde} → ${hasta})`);
  return resultado;
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
