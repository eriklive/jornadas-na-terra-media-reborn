export type Terrain =
  | 'planicie'
  | 'floresta'
  | 'colina'
  | 'montanha'
  | 'agua'
  | 'estrada'
  | 'ruinas'
  | 'vila';

export interface TerrainInfo {
  label: string;
  color: string;
  /** Altura do topo do hexágono, onde os marcadores se apoiam. */
  height: number;
}

export const TERRAINS: Record<Terrain, TerrainInfo> = {
  planicie: { label: 'Planície', color: '#7fa650', height: 0.3 },
  floresta: { label: 'Floresta', color: '#4f7f3a', height: 0.3 },
  colina: { label: 'Colina', color: '#9a9a55', height: 0.36 },
  montanha: { label: 'Montanha', color: '#8a8580', height: 0.4 },
  agua: { label: 'Água', color: '#3f7fb5', height: 0.16 },
  estrada: { label: 'Estrada', color: '#b89a68', height: 0.3 },
  ruinas: { label: 'Ruínas', color: '#8f9488', height: 0.3 },
  vila: { label: 'Vila', color: '#a8905c', height: 0.3 },
};

/** Um hexágono dentro de uma peça. Hexágonos com o mesmo `space` formam um único espaço de jogo. */
export interface CellDef {
  q: number;
  r: number;
  terrain: Terrain;
  space: number;
}

/** Definição de uma peça de mapa: contorno = conjunto de hexágonos; divisão = campo `space`. */
export interface TileDef {
  id: string;
  name: string;
  /** Lado da peça física, ou 'avulso' para o hexágono solto. */
  side: 'A' | 'B' | 'avulso';
  cells: CellDef[];
}

export interface PlacedTile {
  uid: number;
  defId: string;
  q: number;
  r: number;
  rot: number;
  /** Terreno pintado por hexágono (índice em TileDef.cells). Sem entrada = terreno padrão. */
  terrains?: Record<number, Terrain>;
}

export type MarkerKind = 'heroi' | 'inimigo' | 'busca' | 'ameaca' | 'pessoa';

export interface MarkerInfo {
  label: string;
  color: string;
}

export const HERO_COLORS = ['#d9483b', '#3b7dd9', '#3fae5a', '#e0b030', '#9a55c9', '#e8e4d8'];

export const MARKERS: Record<MarkerKind, MarkerInfo> = {
  heroi: { label: 'Herói', color: '#3b7dd9' },
  inimigo: { label: 'Inimigo', color: '#7a1f1f' },
  busca: { label: 'Busca', color: '#e0b030' },
  ameaca: { label: 'Ameaça', color: '#8a3fb0' },
  pessoa: { label: 'Pessoa', color: '#2fa3a3' },
};

export interface PlacedMarker {
  uid: number;
  kind: MarkerKind;
  q: number;
  r: number;
  color: string;
}

export interface MapState {
  version: 1;
  nextUid: number;
  tiles: PlacedTile[];
  markers: PlacedMarker[];
}

export type Mode = 'peca' | 'terreno' | 'marcador' | 'apagar';
