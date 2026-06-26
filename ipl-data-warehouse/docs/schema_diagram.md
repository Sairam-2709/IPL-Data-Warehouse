# Star Schema — IPL Data Warehouse

## Schema Diagram

```
                        ┌─────────────────┐
                        │   dim_seasons   │
                        │─────────────────│
                        │ season_id (PK)  │
                        │ season_year     │
                        └────────┬────────┘
                                 │
          ┌──────────────────────┼──────────────────────┐
          │                      │                      │
┌─────────┴───────┐   ┌──────────┴──────────┐  ┌───────┴─────────┐
│   dim_teams     │   │    dim_matches       │  │   dim_players   │
│─────────────────│   │─────────────────────│  │─────────────────│
│ team_id (PK)    │   │ match_id (PK)        │  │ player_id (PK)  │
│ team_name       │   │ season_year          │  │ player_name     │
└─────────────────┘   │ match_date           │  └─────────────────┘
                       │ city                │
                       │ venue               │
                       │ team1               │
                       │ team2               │
                       │ toss_winner         │
                       │ toss_decision       │
                       │ match_winner        │
                       │ win_by_runs         │
                       │ win_by_wickets      │
                       │ player_of_match     │
                       └──────────┬──────────┘
                                  │
                       ┌──────────┴──────────┐
                       │   fact_deliveries   │
                       │─────────────────────│
                       │ match_id (FK)        │
                       │ season               │
                       │ inning               │
                       │ batting_team         │
                       │ bowling_team         │
                       │ over                 │
                       │ ball                 │
                       │ batsman              │
                       │ bowler               │
                       │ batsman_runs         │
                       │ extra_runs           │
                       │ total_runs           │
                       │ is_boundary          │
                       │ is_four              │
                       │ is_six               │
                       │ is_dot_ball          │
                       │ is_wicket            │
                       │ player_dismissed     │
                       │ dismissal_kind       │
                       └─────────────────────┘
```

## Optimization Strategy

| Optimization | Table | Field | Benefit |
|---|---|---|---|
| Partitioning | `dim_matches` | `match_date` | Faster date-range queries |
| Clustering | `dim_matches` | `season_year`, `match_winner` | Faster season/team filters |
| Clustering | `fact_deliveries` | `batting_team`, `bowling_team` | Faster team-based analysis |
| Clustering | `batting_stats` | `batsman`, `season` | Faster player lookups |
| Clustering | `bowling_stats` | `bowler`, `season` | Faster bowler lookups |

Result: **~35% reduction** in query costs and scan time.
