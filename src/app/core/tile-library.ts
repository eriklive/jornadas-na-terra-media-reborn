import { TileDef } from './models';
import { TILE_OUTLINES } from './tile-outlines';

/**
 * Biblioteca de peças.
 *
 * Cada peça é só um contorno: o conjunto de hexágonos que ela ocupa. Todo hexágono nasce como
 * planície e é um espaço próprio; o terreno de cada um é pintado no editor (modo "Terreno").
 * Para agrupar hexágonos num mesmo espaço de jogo, dê a eles o mesmo valor de `space`.
 */
export const TILE_LIBRARY: TileDef[] = [
  { id: 'avulso', name: 'Hexágono', side: 'avulso', cells: [{ q: 0, r: 0, terrain: 'planicie', space: 0 }] },
  ...TILE_OUTLINES.map(
    (t): TileDef => ({
      id: t.id,
      name: t.id,
      side: t.side,
      cells: t.cells.map(([q, r], i) => ({ q, r, terrain: 'planicie', space: i })),
    }),
  ),
];

export const TILE_BY_ID = new Map(TILE_LIBRARY.map((t) => [t.id, t]));
