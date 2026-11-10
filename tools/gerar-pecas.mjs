// Converte os contornos de peças do JiMEditor (https://github.com/GlowPuff/JiMEditor, GPL-3.0)
// para as coordenadas axiais usadas neste projeto. Só os contornos são lidos; nenhuma imagem é usada.
// Uso: node tools/gerar-pecas.mjs <pasta Assets do JiMEditor> > src/app/core/tile-outlines.ts
import { readFileSync } from 'node:fs';
import { join } from 'node:path';

const dir = process.argv[2];
const S3 = Math.sqrt(3);

function parse(file) {
  const txt = readFileSync(join(dir, file), 'utf8');
  return [...txt.matchAll(/id\s*:\s*(\d+)\s*,\s*coords\s*:\s*"([^"]+)"/g)].map((m) => ({
    id: +m[1],
    cells: m[2].trim().split(/\s+/).map((p) => p.split(',').map(Number)),
  }));
}

function roundAxial(x, z) {
  const fr = z / 1.5, fq = x / S3 - fr / 2, fs = -fq - fr;
  let q = Math.round(fq), r = Math.round(fr);
  const s = Math.round(fs);
  const dq = Math.abs(q - fq), dr = Math.abs(r - fr), ds = Math.abs(s - fs);
  if (dq > dr && dq > ds) q = -r - s; else if (dr > ds) r = -q - s;
  return [q, r];
}

function convert(cells) {
  // Origem: colunas/linhas "dobradas" de hexágonos flat-top. Posição no plano (y para baixo):
  const pts = cells.map(([c, row]) => [1.5 * c, (S3 / 2) * row]);
  const [ox, oy] = pts[0];
  // Gira 30° para o nosso layout pointy-top (rotação, não espelhamento, para manter a quiralidade).
  const a = Math.PI / 6, ca = Math.cos(a), sa = Math.sin(a);
  let ax = pts.map(([x, y]) => {
    const dx = x - ox, dy = y - oy;
    return roundAxial(dx * ca - dy * sa, dx * sa + dy * ca);
  });
  // Recentraliza no hexágono mais próximo do centroide, que vira o pivô de rotação.
  const w = ax.map(([q, r]) => [S3 * (q + r / 2), 1.5 * r]);
  const cx = w.reduce((s, p) => s + p[0], 0) / w.length, cz = w.reduce((s, p) => s + p[1], 0) / w.length;
  let best = 0, bd = Infinity;
  w.forEach((p, i) => { const d = (p[0] - cx) ** 2 + (p[1] - cz) ** 2; if (d < bd) { bd = d; best = i; } });
  const [bq, br] = ax[best];
  ax = ax.map(([q, r]) => [q - bq, r - br]);
  if (new Set(ax.map(String)).size !== cells.length) throw new Error('colisão na conversão');
  return ax;
}

const out = [];
for (const [side, file] of [['A', 'hextilesA.json'], ['B', 'hextilesB.json']]) {
  for (const t of parse(file)) out.push({ id: `${t.id}${side}`, side, cells: convert(t.cells) });
}
console.log('// Gerado por tools/gerar-pecas.mjs — não editar à mão.');
console.log('// Contornos (conjuntos de hexágonos) derivados dos dados do JiMEditor (GPL-3.0).');
console.log("export const TILE_OUTLINES: { id: string; side: 'A' | 'B'; cells: [number, number][] }[] = [");
for (const t of out) console.log(`  { id: '${t.id}', side: '${t.side}', cells: ${JSON.stringify(t.cells)} },`);
console.log('];');
