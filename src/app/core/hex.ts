/** Matemática de grade hexagonal (coordenadas axiais, hexágonos "pointy-top", raio 1). */
export interface Axial {
  q: number;
  r: number;
}

export const SQRT3 = Math.sqrt(3);

/** Vizinhos em ordem angular: o índice i aponta para o ângulo i * 60° no plano XZ. */
export const DIRS: readonly Axial[] = [
  { q: 1, r: 0 },
  { q: 0, r: 1 },
  { q: -1, r: 1 },
  { q: -1, r: 0 },
  { q: 0, r: -1 },
  { q: 1, r: -1 },
];

export const key = (q: number, r: number): string => `${q},${r}`;

export function axialToWorld(q: number, r: number): { x: number; z: number } {
  return { x: SQRT3 * (q + r / 2), z: 1.5 * r };
}

export function worldToAxial(x: number, z: number): Axial {
  const fr = z / 1.5;
  const fq = x / SQRT3 - fr / 2;
  const fs = -fq - fr;
  let q = Math.round(fq);
  let r = Math.round(fr);
  const s = Math.round(fs);
  const dq = Math.abs(q - fq);
  const dr = Math.abs(r - fr);
  const ds = Math.abs(s - fs);
  if (dq > dr && dq > ds) q = -r - s;
  else if (dr > ds) r = -q - s;
  return { q, r };
}

/** Gira uma coordenada em passos de 60° ao redor da origem. */
export function rotate(c: Axial, steps: number): Axial {
  let { q, r } = c;
  const n = ((steps % 6) + 6) % 6;
  for (let i = 0; i < n; i++) [q, r] = [-r, q + r];
  return { q, r };
}

/** Gerador pseudoaleatório determinístico, para a decoração não "pular" a cada redesenho. */
export function seededRandom(q: number, r: number, salt = 0): () => number {
  let a = (q * 73856093) ^ (r * 19349663) ^ (salt * 83492791);
  return () => {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
