# Network Analytics Enablement

Estimate 5G coverage quality for buildings in downtown Seattle by combining FCC broadband data, Microsoft building footprints, and OpenCellID cell tower locations — all using Databricks spatial SQL (H3 + ST functions).

## Architecture

```
  Unity Catalog Volume                   Delta Tables                        Analysis & Output
 (raw_data/)                        (Unity Catalog)
 ┌──────────────────────┐          ┌─────────────────────┐          ┌──────────────────────────────┐
 │ bdc_53_5GNR_...zip   │──────▶  │ fcc_bdc_h3_seattle  │──┐       │ Spatial joins (H3 + ST)      │
 │ (FCC BDC GeoPackage) │  01     │                     │  │       │ Nearest tower (haversine)    │
 ├──────────────────────┤ Ingest  ├─────────────────────┤  │  02   │ Distance vs signal analysis  │
 │ Washington.zip       │──────▶  │ building_footprints │──┼──▶    │ Interactive heatmap (folium) │
 │ (MS Building .shp)   │         │                     │  │ Anal. │                              │
 ├──────────────────────┤         ├─────────────────────┤  │       │ ┌──────────────────────────┐ │
 │ 310.csv.gz           │──────▶  │ cell_towers         │──┘       │ │ downtown_seattle_        │ │
 │ (OpenCellID towers)  │         │ (T-Mobile only)     │          │ │ building_coverage        │ │
 └──────────────────────┘         └─────────────────────┘          │ └──────────────────────────┘ │
                                                                   └──────────────────────────────┘
```

## Notebooks

| Notebook | Purpose |
|---|---|
| `00_instructions.ipynb` | Data source links and setup notes |
| `01_Ingest.ipynb` | Reads zip/csv/gpkg files from a UC Volume, filters to Seattle, writes three Delta tables |
| `02_Analysis.ipynb` | Joins buildings with H3 coverage + nearest tower, produces distance-vs-signal analysis and a folium heatmap |

## Data Sources

| Source | File | Filtered to |
|---|---|---|
| [FCC Broadband Data Collection](https://broadbandmap.fcc.gov/data-download/nationwide-data) | 5G NR H3 hexagon coverage (WA state) | Seattle metro bbox |
| [Microsoft Building Footprints](https://wiki.openstreetmap.org/wiki/Microsoft_Building_Footprint_Data) | Washington state shapefile | Seattle metro bbox |
| [OpenCellID](https://community.opencellid.org/t/how-to-download-cells-database-csv/22) | MCC 310 (USA) tower records | T-Mobile (MNC 260), Seattle metro |

## Genie Code Reproduction

The `notebooks_instructions_only/` folder contains the same notebooks but with **only markdown instructions** (no code cells). These are meant to be opened in Databricks and reproduced entirely using **Genie Code** — you provide the instructions, Genie Code writes and runs the code.

The `(Clone) notebooks_instructions_only/` folder is a second pass at this workflow, showing another attempt at using Genie Code to generate the notebooks from instructions. It includes Genie Code's "Refined Plan" markdown cells showing how the agent interpreted the instructions before writing code.
