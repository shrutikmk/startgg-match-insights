# SmashBot Match Insights

## TL;DR
Analyzes Super Smash Bros. Ultimate tournament data to produce player rankings and auto-generated match writeups.

## Why this matters
- Real applied DS: scraping/cleaning, ranking models, and LLM/NLP writeups
- Demonstrates SQL/Python, features/metrics, and end-to-end pipeline thinking
- Bridges stats and narrative for esports audiences

## Architecture / Approach (draft)
- Ingest start.gg results → clean + normalize
- Rank players with ELO/Glicko + strength-of-field adjustments
- Generate natural-language recaps with prompt templates + evals
- Visualize trends (placement %, notable wins, MU deltas)

## Roadmap
- [ ] Data loader for start.gg exports
- [ ] Baseline ranking (ELO/Glicko)
- [ ] Writeup generator (template → LLM)
- [ ] Dashboards (trends, notable wins)
- [ ] Evaluation (consistency, face validity)
