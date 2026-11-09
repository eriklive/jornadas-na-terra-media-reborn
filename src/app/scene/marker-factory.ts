import * as THREE from 'three';
import { MarkerKind } from '../core/models';

const discGeo = new THREE.CylinderGeometry(0.2, 0.22, 0.06, 20);
const bodyGeo = new THREE.ConeGeometry(0.13, 0.42, 14);
const headGeo = new THREE.SphereGeometry(0.09, 14, 10);
const spikeGeo = new THREE.OctahedronGeometry(0.17);
const ringGeo = new THREE.TorusGeometry(0.1, 0.03, 8, 18);
const gemGeo = new THREE.TetrahedronGeometry(0.15);

const dark = new THREE.MeshStandardMaterial({ color: '#2a241c', roughness: 0.6 });
const cache = new Map<string, THREE.MeshStandardMaterial>();
const colored = (color: string) => {
  let m = cache.get(color);
  if (!m) cache.set(color, (m = new THREE.MeshStandardMaterial({ color, roughness: 0.45, metalness: 0.15 })));
  return m;
};

function part(g: THREE.Group, geo: THREE.BufferGeometry, material: THREE.Material, y: number): THREE.Mesh {
  const m = new THREE.Mesh(geo, material);
  m.position.y = y;
  m.castShadow = true;
  g.add(m);
  return m;
}

/** Marcadores genéricos: peão (herói/pessoa), figura pontiaguda (inimigo) e fichas (busca/ameaça). */
export function buildMarker(kind: MarkerKind, color: string): THREE.Group {
  const g = new THREE.Group();
  const c = colored(color);
  switch (kind) {
    case 'heroi':
      part(g, discGeo, dark, 0.03);
      part(g, bodyGeo, c, 0.27);
      part(g, headGeo, c, 0.5);
      break;
    case 'pessoa':
      part(g, discGeo, c, 0.03);
      part(g, bodyGeo, dark, 0.2).scale.setScalar(0.7);
      part(g, headGeo, dark, 0.37).scale.setScalar(0.7);
      break;
    case 'inimigo':
      part(g, discGeo, dark, 0.03);
      part(g, spikeGeo, c, 0.27).scale.set(1, 1.5, 1);
      break;
    case 'busca':
      part(g, discGeo, c, 0.03);
      part(g, ringGeo, dark, 0.075).rotation.x = Math.PI / 2;
      break;
    case 'ameaca':
      part(g, discGeo, c, 0.03);
      part(g, gemGeo, dark, 0.13);
      break;
  }
  return g;
}
