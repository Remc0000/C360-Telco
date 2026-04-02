# Squad Team

> C360-Telco — Telco Customer 360 Medallion Architecture

## Coordinator

| Name | Role | Notes |
|------|------|-------|
| Squad | Coordinator | Routes work, enforces handoffs and reviewer gates. |

## Members

| Name | Role | Charter | Status |
|------|------|---------|--------|
| **Bell** (Alexander Graham Bell) | Project Lead & Communication Architect | [charter](.squad/agents/bell/charter.md) | Active |
| **Marconi** (Guglielmo Marconi) | Senior Data Engineer & Pipeline Builder | [charter](.squad/agents/marconi/charter.md) | Active |
| **Tesla** (Nikola Tesla) | Solution Architect & Innovation Lead | [charter](.squad/agents/tesla/charter.md) | Active |
| **Shannon** (Claude Shannon) | Data Scientist & Analytics Engineer | [charter](.squad/agents/shannon/charter.md) | Active |
| **Cooper** (Martin Cooper) | DevOps Engineer & Automation Specialist | [charter](.squad/agents/cooper/charter.md) | Active |

## Project Context

- **Project:** C360-Telco - Telco Customer 360 Medallion Architecture
- **Created:** 2026-04-02
- **Mission:** Transform 26 raw telco CSV datasets into Bronze → Silver → Gold medallion architecture for highly personalized customer insights
- **Platform:** Microsoft Fabric with fab CLI automation
- **Tech Stack:** PySpark, Delta Lake, Power BI, MLflow

## Team Expertise

- Microsoft Fabric platform and fab CLI mastery
- Medallion architecture (Bronze/Silver/Gold)
- Telco domain knowledge (billing, subscribers, mobile services)
- Customer 360 and personalization platforms
- Data engineering, analytics, and ML
- DevOps and pipeline automation

## Collaboration Model

```
┌─────────────────────────────────────────────────────────────┐
│                         Bell                                 │
│            (Project Lead & Coordinator)                      │
│      Translates business needs ↔ Technical execution        │
└─────────────────────────────────────────────────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        │                   │                   │
    ┌───▼────┐         ┌───▼────┐         ┌───▼────┐
    │ Tesla  │────────▶│Marconi │◀────────│Shannon │
    │        │  Design │        │   Data  │        │
    └───┬────┘         └───┬────┘         └───┬────┘
        │                   │                   │
        └───────────────────┼───────────────────┘
                        ┌───▼────┐
                        │ Cooper │
                        │        │
                        └────────┘
```

- **Bell:** Oversees project, communicates with stakeholders
- **Tesla:** Designs architecture, guides Marconi on implementation
- **Marconi:** Builds data pipelines, implements transformations
- **Shannon:** Designs analytics features, builds ML models
- **Cooper:** Automates deployment, monitors operations
