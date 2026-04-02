# Work Routing

How to decide who handles what in the C360-Telco squad.

## Routing Table

| Work Type | Route To | Examples |
|-----------|----------|----------|
| **Architecture & Design** | Tesla | Medallion layer schema design, scalability planning, technology selection, design patterns |
| **Data Engineering** | Marconi | PySpark notebooks, Delta table creation, pipeline implementation, Bronze/Silver/Gold transformations |
| **Analytics & ML** | Shannon | Gold layer features, churn models, Power BI reports, business metrics, KPI calculations |
| **DevOps & Automation** | Cooper | CI/CD pipelines, fab CLI automation, monitoring, deployment, orchestration, performance tuning |
| **Business & Coordination** | Bell | Stakeholder communication, business requirements, project planning, team coordination, status updates |
| **fab CLI Operations** | Cooper (primary), Marconi (secondary) | Fabric workspace management, lakehouse operations, resource provisioning |
| **Fabric Platform Questions** | Tesla (architecture), Cooper (operations) | Platform capabilities, best practices, Fabric-specific decisions |
| **Telco Domain Logic** | Bell (business), Shannon (analytics) | Customer lifecycle, billing logic, churn definitions, personalization rules |
| **Code Review** | Tesla (architecture), Marconi (implementation) | Review PRs, check quality, suggest improvements, validate patterns |
| **Testing & QA** | Cooper (integration), Shannon (data quality) | Test design, data validation, quality checks, performance benchmarks |
| **Documentation** | Bell | Project docs, ADRs, business context, user guides, stakeholder materials |
| **Scope & Priorities** | Bell | What to build next, trade-offs, prioritization, budget, timeline |
| **Session Logging** | Bell | Automatic — documents decisions and progress |

## Issue Routing

| Label | Action | Who |
|-------|--------|-----|
| `squad` | Triage: analyze issue, assign `squad:{member}` label | Bell (Lead) |
| `squad:bell` | Project coordination, business alignment | Bell |
| `squad:tesla` | Architecture review, design decisions | Tesla |
| `squad:marconi` | Data pipeline implementation | Marconi |
| `squad:shannon` | Analytics, ML models, Power BI | Shannon |
| `squad:cooper` | DevOps, automation, deployment | Cooper |

### How Issue Assignment Works

1. When a GitHub issue gets the `squad` label, **Bell** triages it — analyzing content, assigning the right `squad:{member}` label, and commenting with triage notes.
2. When a `squad:{member}` label is applied, that member picks up the issue in their next session.
3. Members can reassign by removing their label and adding another member's label.
4. The `squad` label is the "inbox" — untriaged issues waiting for Bell's review.

## Specialized Routing Rules

### Data Pipeline Work
1. **Design Phase:** Tesla creates architecture → Marconi reviews for feasibility
2. **Implementation:** Marconi builds → Tesla reviews architecture adherence
3. **Testing:** Cooper validates integration → Shannon checks data quality
4. **Deployment:** Cooper automates → Bell communicates to stakeholders

### Analytics Work
1. **Requirements:** Bell gathers business needs → Shannon translates to features
2. **Design:** Shannon defines metrics → Tesla validates scalability
3. **Implementation:** Shannon builds Gold tables → Marconi optimizes queries
4. **Deployment:** Cooper automates refresh → Bell tracks business impact

### fab CLI Automation
1. **Strategy:** Tesla defines patterns → Cooper implements automation
2. **Execution:** Cooper scripts operations → Marconi validates on real data
3. **Monitoring:** Cooper sets alerts → Bell reviews operational metrics

## Rules

1. **Eager by default** — spawn all agents who could usefully start work, including anticipatory downstream work.
2. **Bell always runs** after substantial work to document decisions and progress.
3. **Quick facts → coordinating agent answers directly.** Don't spawn an agent for "what's the lakehouse name?"
4. **When two agents could handle it**, pick the one whose domain is the primary concern.
5. **"Team, ..." → fan-out.** Spawn all relevant agents in parallel as `mode: "background"`.
6. **Anticipate downstream work:** 
   - If Tesla designs → spawn Marconi to plan implementation
   - If Marconi builds → spawn Shannon to prepare analytics
   - If Shannon models → spawn Cooper to prepare deployment
7. **Issue-labeled work** — when a `squad:{member}` label is applied to an issue, route to that member. Bell handles all `squad` (base label) triage.
8. **fab CLI expertise** — Default to Cooper for operations, but Marconi can handle data-specific commands.

## Common Routing Scenarios

| User Request | Primary Agent | Supporting Agents |
|--------------|---------------|-------------------|
| "Design the Silver layer schema" | Tesla | Marconi (feasibility) |
| "Build Bronze ingestion notebook" | Marconi | Cooper (deployment) |
| "Create churn prediction model" | Shannon | Tesla (architecture), Marconi (data) |
| "Automate pipeline deployment" | Cooper | Marconi (validate) |
| "Explain project to stakeholders" | Bell | All (provide context) |
| "Use fab CLI to create lakehouse" | Cooper | Marconi (if data-focused) |
| "Optimize query performance" | Marconi | Tesla (architecture), Cooper (monitoring) |
| "Build Power BI dashboard" | Shannon | Bell (requirements) |
