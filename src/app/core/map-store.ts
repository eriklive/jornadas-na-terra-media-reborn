import { Injectable, computed, effect, signal } from '@angular/core';
import { DIRS, key, rotate } from './hex';
import { CellDef, HERO_COLORS, MARKERS, MapState, MarkerKind, Mode, PlacedTile, TERRAINS, Terrain } from './models';
import { TILE_BY_ID, TILE_LIBRARY } from './tile-library';

const STORAGE_KEY = 'jtm-mapa-v1';
const EMPTY: MapState = { version: 1, nextUid: 1, tiles: [], markers: [] };

export interface OccupiedCell {
  tile: PlacedTile;
  cell: CellDef;
  /** Índice do hexágono dentro da definição da peça. */
  index: number;
  /** Terreno efetivo (pintado ou padrão). */
  terrain: Terrain;
  q: number;
  r: number;
}

function isMapState(v: unknown): v is MapState {
  const s = v as MapState;
  return (
    !!s &&
    s.version === 1 &&
    Array.isArray(s.tiles) &&
    Array.isArray(s.markers) &&
    s.tiles.every((t) => TILE_BY_ID.has(t.defId) && Number.isInteger(t.q) && Number.isInteger(t.r)) &&
    s.markers.every((m) => m.kind in MARKERS && Number.isInteger(m.q) && Number.isInteger(m.r))
  );
}

function load(): MapState {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    const parsed = raw ? JSON.parse(raw) : null;
    if (isMapState(parsed)) return parsed;
  } catch {
    /* armazenamento indisponível: começa vazio */
  }
  return EMPTY;
}

@Injectable({ providedIn: 'root' })
export class MapStore {
  readonly state = signal<MapState>(load());

  // Ferramenta atual
  readonly mode = signal<Mode>('peca');
  readonly tileId = signal<string>(TILE_LIBRARY[0].id);
  readonly rotation = signal(0);
  readonly paintTerrain = signal<Terrain>('floresta');
  readonly markerKind = signal<MarkerKind>('heroi');
  readonly heroColor = signal(HERO_COLORS[0]);

  private history: MapState[] = [];
  readonly canUndo = signal(false);

  /** Hexágonos ocupados, indexados por "q,r". */
  readonly occupied = computed(() => {
    const map = new Map<string, OccupiedCell>();
    for (const tile of this.state().tiles) {
      for (const oc of this.cellsOf(tile.defId, tile.q, tile.r, tile.rot)) {
        const terrain = tile.terrains?.[oc.index] ?? oc.cell.terrain;
        map.set(key(oc.q, oc.r), { tile, cell: oc.cell, index: oc.index, terrain, q: oc.q, r: oc.r });
      }
    }
    return map;
  });

  constructor() {
    effect(() => {
      const s = this.state();
      try {
        localStorage.setItem(STORAGE_KEY, JSON.stringify(s));
      } catch {
        /* ignora */
      }
    });
  }

  cellsOf(defId: string, q: number, r: number, rot: number): { q: number; r: number; cell: CellDef; index: number }[] {
    const def = TILE_BY_ID.get(defId);
    if (!def) return [];
    return def.cells.map((cell, index) => {
      const o = rotate(cell, rot);
      return { q: q + o.q, r: r + o.r, cell, index };
    });
  }

  canPlace(defId: string, q: number, r: number, rot: number): boolean {
    const occ = this.occupied();
    return this.cellsOf(defId, q, r, rot).every((c) => !occ.has(key(c.q, c.r)));
  }

  /** Altura do topo do terreno em um hexágono (0 se não houver peça). */
  heightAt(q: number, r: number): number {
    const oc = this.occupied().get(key(q, r));
    return oc ? TERRAINS[oc.terrain].height : 0;
  }

  private commit(next: MapState): void {
    this.history.push(this.state());
    if (this.history.length > 100) this.history.shift();
    this.canUndo.set(true);
    this.state.set(next);
  }

  /** Executa a ferramenta atual no hexágono indicado. Retorna false se nada mudou. */
  applyAt(q: number, r: number): boolean {
    const s = this.state();
    switch (this.mode()) {
      case 'peca': {
        const defId = this.tileId();
        const rot = this.rotation();
        if (!this.canPlace(defId, q, r, rot)) return false;
        this.commit({
          ...s,
          nextUid: s.nextUid + 1,
          tiles: [...s.tiles, { uid: s.nextUid, defId, q, r, rot }],
        });
        return true;
      }
      case 'terreno': {
        const oc = this.occupied().get(key(q, r));
        const terrain = this.paintTerrain();
        if (!oc || oc.terrain === terrain) return false;
        this.commit({
          ...s,
          tiles: s.tiles.map((t) =>
            t.uid === oc.tile.uid ? { ...t, terrains: { ...t.terrains, [oc.index]: terrain } } : t,
          ),
        });
        return true;
      }
      case 'marcador': {
        const kind = this.markerKind();
        const color = kind === 'heroi' ? this.heroColor() : MARKERS[kind].color;
        this.commit({
          ...s,
          nextUid: s.nextUid + 1,
          markers: [...s.markers, { uid: s.nextUid, kind, q, r, color }],
        });
        return true;
      }
      case 'apagar': {
        // Primeiro o marcador mais recente do hexágono; se não houver, a peça.
        for (let i = s.markers.length - 1; i >= 0; i--) {
          if (s.markers[i].q === q && s.markers[i].r === r) {
            this.commit({ ...s, markers: s.markers.filter((_, j) => j !== i) });
            return true;
          }
        }
        const oc = this.occupied().get(key(q, r));
        if (!oc) return false;
        this.commit({ ...s, tiles: s.tiles.filter((t) => t.uid !== oc.tile.uid) });
        return true;
      }
    }
  }

  rotateTool(steps = 1): void {
    this.rotation.update((r) => (((r + steps) % 6) + 6) % 6);
  }

  undo(): void {
    const prev = this.history.pop();
    if (prev) this.state.set(prev);
    this.canUndo.set(this.history.length > 0);
  }

  clear(): void {
    const s = this.state();
    if (s.tiles.length || s.markers.length) this.commit({ ...EMPTY });
  }

  exportJson(): string {
    return JSON.stringify(this.state(), null, 2);
  }

  importJson(text: string): boolean {
    try {
      const parsed = JSON.parse(text);
      if (!isMapState(parsed)) return false;
      this.commit(parsed);
      return true;
    } catch {
      return false;
    }
  }

  /** Para cada hexágono ocupado, que tipo de borda desenhar em cada uma das 6 direções. */
  edgeKind(oc: OccupiedCell, dir: number): 'contorno' | 'espaco' | null {
    const d = DIRS[dir];
    const n = this.occupied().get(key(oc.q + d.q, oc.r + d.r));
    if (!n || n.tile.uid !== oc.tile.uid) return 'contorno';
    return n.cell.space !== oc.cell.space ? 'espaco' : null;
  }
}
