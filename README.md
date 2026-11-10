# AggieSB+ Backend

[![courses upsert](https://cronitor.io/badges/CX7Ga9/production/PC1VqKFw9Yu8A5XVaQYPIlzpt8M.svg)](https://cronitor.io)
[![gpa upsert](https://cronitor.io/badges/PpJGqD/production/5dERFenglEK7EEHI_IIF7YOerDo.svg)](https://cronitor.io)
[![sections upsert](https://cronitor.io/badges/YStGRp/production/gWxQMri7OHIMYTz3F7jFmar2hS4.svg)](https://cronitor.io)
[![sections watcher](https://cronitor.io/badges/FnepfT/production/zqE7gN84msH6qtSME2AqAYzXWVs.svg)](https://cronitor.io)

The API and data pipelines behind [AggieSB+](https://aggieschedulebuilderplus.vercel.app), a course and professor lookup tool for Texas A&M students.

**API docs:** https://api-aggiesbp.nitheeshk.com/docs · **Frontend:** [AggieSBP-Frontend](https://github.com/nitheesh-cpu/AggieSBP-Frontend) · **Extension:** [AggieSBP-Extension](https://github.com/nitheesh-cpu/AggieSBP-Extension)

~6.3k professors · 5.6k courses · 140k sections · 97k reviews · 163k grade records

## What's in here

- Course, professor and section APIs with search and compare
- Grade distributions and RateMyProfessors reviews side by side
- AI review summaries that cite the reviews they came from
- Easiest core classes, GPA boosters, and "fit my schedule" (find sections that don't conflict)
- Prerequisite trees for the schedule builder
- Seat alerts over Web Push

## Architecture

```mermaid
graph TD
    %% -- Styling --
    classDef external fill:#f9f,stroke:#333,stroke-width:2px,color:#000;
    classDef storage fill:#555,stroke:#333,stroke-dasharray: 5 5,color:#fff;
    classDef client fill:#4a90e2,stroke:#333,stroke-width:2px,color:#fff;
    classDef auth fill:#2e7d32,stroke:#333,stroke-width:2px,color:#fff;
    classDef default fill:#333,color:#fff,stroke:#111;

    %% -- Entities --
    User["TAMU Student (Browser)"]:::client

    subgraph ExternalSources ["External Data Sources"]
        direction LR
        TAMU["TAMU Catalog + Sections"]:::external
        RMP["RateMyProfessors"]:::external
        Anex["Anex.us (GPA)"]:::external
        LLM["LLM API"]:::external
    end

    subgraph ClientLayer ["Client Side"]
        NextJS["Next.js Web App (Vercel)"]

        subgraph Extension ["Browser Extension (Chrome / Firefox)"]
            ContentScript["Content Scripts (College Scheduler, Howdy)"]
            Popup["Extension Popup (UI)"]
        end
    end

    subgraph AuthLayer ["Auth (SuperTokens)"]
        SuperTokens["SuperTokens: sessions, email/password, JWT"]:::auth
    end

    subgraph ServiceLayer ["Application Layer"]
        FastAPI["AggieSB+ API (FastAPI/Python)"]
    end

    subgraph WorkerLayer ["Data Pipelines (Scheduled)"]
        Scrapers["Scraper Workers (TAMU, RMP, GPA)"]
        Summarizer["Review Summarizer (cited LLM summaries)"]
        SectionWatcher["Section Watcher: seat-open alerts + Web Push"]
    end

    subgraph StorageLayer ["Storage Layer"]
        PostgreSQL[(PostgreSQL Main DB)]:::storage
        Redis[(Redis: cache + tracked sections)]:::storage
    end

    %% -- Connections --
    User -->|Interacts with| NextJS
    User -->|Uses| Popup
    ContentScript -.->|Injects UI into| User
    ContentScript -->|Howdy course import| NextJS

    NextJS <-->|Auth APIs| SuperTokens
    FastAPI <-->|verify_session + recipe| SuperTokens

    NextJS <-->|REST| FastAPI
    Popup <-->|Fetch Stats| FastAPI
    ContentScript <-->|Fetch Prof Metrics + Seat Alerts| FastAPI

    FastAPI <-->|SQL Queries| PostgreSQL
    FastAPI <-->|Cache / tracking sets| Redis

    Scrapers -.->|Scrape / Download| ExternalSources
    Scrapers -->|ETL / Upsert| PostgreSQL
    Summarizer -->|Reviews in| PostgreSQL
    Summarizer -.->|Structured summaries| LLM

    SectionWatcher -->|Active watches + section status| PostgreSQL
    SectionWatcher -.->|Web Push when seat opens| User
```

## Pipelines

| Pipeline | Status |
| --- | --- |
| Sections upsert | <img width="50%" src="https://cronitor.io/badges/YStGRp/production/gWxQMri7OHIMYTz3F7jFmar2hS4/detailed.svg" alt="sections upsert" /> |
| GPA upsert | <img width="50%" src="https://cronitor.io/badges/PpJGqD/production/5dERFenglEK7EEHI_IIF7YOerDo/detailed.svg" alt="gpa upsert" /> |
| Courses upsert | <img width="50%" src="https://cronitor.io/badges/CX7Ga9/production/PC1VqKFw9Yu8A5XVaQYPIlzpt8M/detailed.svg" alt="courses upsert" /> |
| Sections watcher | <img width="50%" src="https://cronitor.io/badges/FnepfT/production/zqE7gN84msH6qtSME2AqAYzXWVs/detailed.svg" alt="sections watcher" /> |

| Source | Data |
| --- | --- |
| TAMU Course Catalog | Courses, descriptions, prerequisites |
| Howdy class search | Sections, meeting times, instructors, seats |
| Anex.us | Grade distributions |
| RateMyProfessors | Ratings and reviews (College Station + Galveston) |

## Stack

Python, FastAPI, PostgreSQL, Redis, SQLAlchemy, SuperTokens, Playwright, pywebpush. CI runs ruff, mypy and pytest.

## Run it locally

You need Python 3.10+, Postgres, and optionally Redis and a SuperTokens core.

```bash
git clone https://github.com/nitheesh-cpu/AggieSBP-Backend.git
cd AggieSBP-Backend
pip install -r requirements.txt
cp .env.example .env   # fill in your DB, Redis and SuperTokens settings
python run_api.py      # http://localhost:8000/docs
```

Pipelines:

```bash
python -m pipelines.courses.upsert_courses
python -m pipelines.sections.upsert_all
python pipelines/gpa/upsert_gpa_data.py
python -m pipelines.professors.upsert_all
python -m aggiermp.collectors.section_watcher
```

## Layout

```
src/aggiermp/api/          FastAPI app and routers
src/aggiermp/core/         config, cache, name matching, push
src/aggiermp/collectors/   seat-alert watcher
pipelines/                 scrapers + upserts (courses, sections, gpa, professors)
tests/                     pytest
```

MIT License
