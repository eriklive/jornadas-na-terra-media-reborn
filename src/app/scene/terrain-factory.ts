import * as THREE from 'three';
import { DIRS, SQRT3, axialToWorld, seededRandom } from '../core/hex';
import { TERRAINS, Terrain } from '../core/models';

/** Geometrias e materiais compartilhados entre todos os hexágonos. */
const hexGeo = new THREE.CylinderGeometry(1, 1, 1, 6);
const coneGeo = new THREE.ConeGeometry(1, 1, 7);
const peakGeo = new THREE.ConeGeometry(1, 1, 5);
const sphereGeo = new THREE.SphereGeometry(1, 10, 8);
const boxGeo = new THREE.BoxGeometry(1, 1, 1);
const roofGeo = new THREE.ConeGeometry(1, 1, 4);
const trunkGeo = new THREE.CylinderGeometry(1, 1, 1, 6);

const mat = (color: string, extra: THREE.MeshStandardMaterialParameters = {}) =>
  new THREE.MeshStandardMaterial({ color, roughness: 0.9, flatShading: true, ...extra });

const baseMats = Object.fromEntries(
  (Object.keys(TERRAINS) as Terrain[]).map((t) => [
    t,
    t === 'agua' ? mat(TERRAINS[t].color, { roughness: 0.25, metalness: 0.1 }) : mat(TERRAINS[t].color),
  ]),
) as Record<Terrain, THREE.MeshStandardMaterial>;

const M = {
  soil: mat('#5b4a36'),
  leaf: mat('#2f6b34'),
  leafDark: mat('#255a2c'),
  trunk: mat('#6b4a2b'),
  hill: mat('#86904a'),
  rock: mat('#77726d'),
  snow: mat('#eef1f3'),
  stone: mat('#b3b6ab'),
  path: mat('#8f744b'),
  wall: mat('#d9c9a3'),
  roof: mat('#9c4a32'),
  outline: mat('#2a241c'),
  divider: mat('#f1e7cf'),
};

function add(
  parent: THREE.Object3D,
  geo: THREE.BufferGeometry,
  material: THREE.Material,
  x: number,
  y: number,
  z: number,
  sx: number,
  sy: number,
  sz: number,
  rotY = 0,
): THREE.Mesh {
  const m = new THREE.Mesh(geo, material);
  m.position.set(x, y, z);
  m.scale.set(sx, sy, sz);
  m.rotation.y = rotY;
  m.castShadow = true;
  m.receiveShadow = true;
  parent.add(m);
  return m;
}

/** Pontos num anel ao redor do centro, deixando o meio livre para os marcadores. */
function ring(rnd: () => number, count: number, rMin = 0.42, rMax = 0.68): [number, number][] {
  const start = rnd() * Math.PI * 2;
  return Array.from({ length: count }, (_, i) => {
    const a = start + (i / count) * Math.PI * 2 + (rnd() - 0.5) * 0.5;
    const r = rMin + rnd() * (rMax - rMin);
    return [Math.cos(a) * r, Math.sin(a) * r];
  });
}

function decorate(g: THREE.Group, terrain: Terrain, top: number, rnd: () => number): void {
  switch (terrain) {
    case 'floresta':
      for (const [x, z] of ring(rnd, 7)) {
        const h = 0.45 + rnd() * 0.35;
        add(g, trunkGeo, M.trunk, x, top + 0.08, z, 0.04, 0.16, 0.04);
        add(g, coneGeo, rnd() > 0.5 ? M.leaf : M.leafDark, x, top + 0.14 + h / 2, z, 0.17, h, 0.17, rnd() * 6);
      }
      break;
    case 'colina':
      for (const [x, z] of ring(rnd, 3, 0.3, 0.5)) {
        const s = 0.32 + rnd() * 0.16;
        add(g, sphereGeo, M.hill, x, top - 0.02, z, s, s * 0.55, s);
      }
      break;
    case 'montanha':
      for (const [x, z] of ring(rnd, 3, 0.38, 0.5)) {
        const h = 0.8 + rnd() * 0.6;
        const w = 0.34 + rnd() * 0.1;
        const rot = rnd() * 6;
        add(g, peakGeo, M.rock, x, top + h / 2, z, w, h, w, rot);
        add(g, peakGeo, M.snow, x, top + h * 0.86, z, w * 0.3, h * 0.29, w * 0.3, rot);
      }
      break;
    case 'agua':
      for (const [x, z] of ring(rnd, 2, 0.5, 0.7)) {
        add(g, sphereGeo, M.rock, x, top, z, 0.09, 0.06, 0.09);
      }
      break;
    case 'estrada':
      add(g, hexGeo, M.path, 0, top + 0.005, 0, 0.55, 0.02, 0.55);
      for (const [x, z] of ring(rnd, 5, 0.66, 0.78)) {
        add(g, sphereGeo, M.stone, x, top, z, 0.06, 0.04, 0.06);
      }
      break;
    case 'ruinas':
      for (const [x, z] of ring(rnd, 5, 0.5, 0.66)) {
        const h = 0.15 + rnd() * 0.5;
        add(g, boxGeo, M.stone, x, top + h / 2, z, 0.14, h, 0.14, rnd() * 0.6);
      }
      add(g, boxGeo, M.stone, 0.45, top + 0.05, 0.1, 0.5, 0.1, 0.12, 1.1);
      break;
    case 'vila':
      for (const [x, z] of ring(rnd, 4, 0.5, 0.62)) {
        const rot = rnd() * 6;
        const w = 0.2 + rnd() * 0.06;
        add(g, boxGeo, M.wall, x, top + 0.1, z, w, 0.2, w, rot);
        add(g, roofGeo, M.roof, x, top + 0.29, z, w * 0.85, 0.18, w * 0.85, rot + Math.PI / 4);
      }
      break;
    case 'planicie':
      for (const [x, z] of ring(rnd, 3, 0.5, 0.75)) {
        add(g, coneGeo, M.leaf, x, top + 0.04, z, 0.05, 0.08, 0.05);
      }
      break;
  }
}

export type EdgeKind = 'contorno' | 'espaco' | null;

/**
 * Constrói um hexágono 3D: base, decoração do terreno e as bordas
 * (contorno da peça em escuro, divisão entre espaços em claro).
 */
export function buildCell(q: number, r: number, terrain: Terrain, edges: EdgeKind[]): THREE.Group {
  const g = new THREE.Group();
  const { x, z } = axialToWorld(q, r);
  g.position.set(x, 0, z);
  const top = TERRAINS[terrain].height;

  add(g, hexGeo, M.soil, 0, 0.04, 0, 1, 0.08, 1);
  add(g, hexGeo, baseMats[terrain], 0, 0.08 + (top - 0.08) / 2, 0, 0.995, top - 0.08, 0.995);
  decorate(g, terrain, top, seededRandom(q, r, terrain.length));

  edges.forEach((kind, i) => {
    if (!kind) return;
    const a = (i * Math.PI) / 3;
    const d = (SQRT3 / 2) * (kind === 'contorno' ? 0.965 : 0.985);
    const thick = kind === 'contorno' ? 0.07 : 0.03;
    const e = add(
      g,
      boxGeo,
      kind === 'contorno' ? M.outline : M.divider,
      Math.cos(a) * d,
      top + 0.012,
      Math.sin(a) * d,
      1.0,
      0.03,
      thick,
      -(a + Math.PI / 2),
    );
    e.castShadow = false;
  });
  return g;
}

/** Hexágono translúcido usado na pré-visualização. */
const ghostGeo = new THREE.CylinderGeometry(0.97, 0.97, 0.36, 6);
export const ghostOk = new THREE.MeshBasicMaterial({ color: '#f5d76e', transparent: true, opacity: 0.45, depthWrite: false });
export const ghostBad = new THREE.MeshBasicMaterial({ color: '#e04b3a', transparent: true, opacity: 0.45, depthWrite: false });

export function buildGhostCell(q: number, r: number, material: THREE.Material): THREE.Mesh {
  const m = new THREE.Mesh(ghostGeo, material);
  const { x, z } = axialToWorld(q, r);
  m.position.set(x, 0.2, z);
  return m;
}

/** Linhas da grade hexagonal de fundo. */
export function buildGrid(radius: number): THREE.LineSegments {
  const pts: number[] = [];
  for (let q = -radius; q <= radius; q++) {
    for (let r = Math.max(-radius, -q - radius); r <= Math.min(radius, -q + radius); r++) {
      const { x, z } = axialToWorld(q, r);
      for (let i = 0; i < 6; i++) {
        const a0 = Math.PI / 6 + (i * Math.PI) / 3;
        const a1 = a0 + Math.PI / 3;
        pts.push(x + Math.cos(a0), 0.01, z + Math.sin(a0), x + Math.cos(a1), 0.01, z + Math.sin(a1));
      }
    }
  }
  const geo = new THREE.BufferGeometry();
  geo.setAttribute('position', new THREE.Float32BufferAttribute(pts, 3));
  return new THREE.LineSegments(geo, new THREE.LineBasicMaterial({ color: '#6f6248', transparent: true, opacity: 0.55 }));
}

export { DIRS };
