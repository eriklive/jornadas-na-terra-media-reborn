# Jornadas na Terra Média Reborn — mapa 3D

Editor de mapas hexagonais em 3D feito com **Angular** e **Three.js**, pensado para montar os mapas das partidas.

**Abrir o editor:** https://eriklive.github.io/jornadas-na-terra-media-reborn/

## O que já faz

- **Peças**: os 22 contornos de peça (lados A e B) mais um hexágono avulso. Encaixe na grade com rotação de 60° e bloqueio de sobreposição.
- **Terreno**: cada hexágono de uma peça pode ser pintado como planície, floresta, colina, montanha, água, estrada, ruínas ou vila. Todo o relevo é gerado por código.
- **Marcadores**: heróis (6 cores), inimigos, busca, ameaça e pessoa. Vários marcadores no mesmo hexágono se espalham em círculo.
- **Apagar**, **Desfazer**, salvamento automático no navegador e **exportar/importar** o mapa em JSON.

Controles: arrastar move o mapa; dois dedos (ou botão direito e roda do mouse) giram e aproximam; `R` gira a peça; `Ctrl+Z` desfaz.

## Presets de peça

Um preset define o terreno padrão de cada hexágono de uma peça; ao colocar a peça no mapa ela já nasce pintada. No menu **Mapa**:

- **Salvar pinturas como padrão**: grava a pintura das peças que estão no mapa como padrão de cada uma.
- **Exportar / Importar presets**: salva e carrega um arquivo `presets.json`.

Formato (o índice do hexágono é a posição dele em `tile-outlines.ts`; o jeito mais fácil de obter o arquivo é pintar e exportar):

```json
{
  "version": 1,
  "presets": {
    "100A": { "0": "floresta", "2": "agua" },
    "300B": { "5": "ruinas" }
  }
}
```

Terrenos aceitos: `planicie`, `floresta`, `colina`, `montanha`, `agua`, `estrada`, `ruinas`, `vila`. Hexágonos sem entrada ficam como planície.

## Rodar

```bash
npm install
npm start        # http://localhost:4200
npm run build
```

## Publicação

Cada push na `main` roda `.github/workflows/publicar.yml`, que gera o build e o envia para a branch `gh-pages`, servida pelo GitHub Pages.

## Estrutura

| Caminho | Conteúdo |
| --- | --- |
| `src/app/core/hex.ts` | Matemática da grade (coordenadas axiais) |
| `src/app/core/tile-outlines.ts` | Contornos das peças (gerado) |
| `src/app/core/tile-library.ts` | Biblioteca de peças usada pelo editor |
| `src/app/core/map-store.ts` | Estado do mapa, ferramentas, desfazer, salvar/carregar |
| `src/app/scene/` | Cena Three.js, terrenos e marcadores 3D |
| `tools/gerar-pecas.mjs` | Script que gera `tile-outlines.ts` |

## Sobre as peças

Este projeto **não contém nem usa a arte oficial do jogo**. De cada peça existe apenas o contorno (quais hexágonos ela ocupa); o visual 3D é modelagem própria e o terreno é escolhido por quem monta o mapa.

Os contornos foram derivados dos dados de layout do [JiMEditor](https://github.com/GlowPuff/JiMEditor) (GPL-3.0), convertidos por `tools/gerar-pecas.mjs`.

Projeto de fã, sem vínculo com os detentores dos direitos do jogo.
